/**
 * MISSION CONTROL WORKER
 * Production-safe Durable Object architecture
 * Authoritative audit append to D1 (audit_logs)
 */

const MAX_PROCESSED_EVENTS = 1000;
const BATCH_THRESHOLD = 50;

const MISSION_FSM = {
  draft: ["submitted"],
  submitted: ["confirmed", "cancelled"],
  confirmed: ["completed"],
  cancelled: [],
  completed: []
};

const TERMINAL_STATES = ["completed", "cancelled"];

const MISSION_EVENTS = {
  STATUS_CHANGED: "MISSION_STATUS_CHANGED",
  SLA_SCHEDULED: "SLA_SCHEDULED",
  SLA_CANCELLED: "SLA_CANCELLED",
  PRESENCE_UPDATE: "PRESENCE_UPDATE",
  SNAPSHOT_SYNC: "SNAPSHOT_SYNC"
};

const json = (data, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });

/* =========================================================
   MISSION STATE DURABLE OBJECT
========================================================= */

export class MissionState {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    this.presence = new Map();
    this.timers = new Map();
    this.processedEvents = new Map();
    this.missionCache = null;
    this.eventCounter = 0;

    state.blockConcurrencyWhile(async () => {
      const [timers, events, cache] = await Promise.all([
        state.storage.get("timers"),
        state.storage.get("processedEvents"),
        state.storage.get("missionCache")
      ]);

      this.timers = new Map(timers || []);
      this.processedEvents = new Map(events || []);
      this.missionCache = cache || { status: "draft", version: 0 };
    });
  }

  /* ================================
     D1 AUDIT APPEND (EXACT SCHEMA)
  ================================= */

  async appendAuditEvent(event) {
    if (!this.env.DB) {
      throw new Error("D1 binding DB not configured");
    }

    await this.env.DB
      .prepare(`
        INSERT INTO audit_logs (
          id,
          event_type,
          actor,
          organization_id,
          payload,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
      `)
      .bind(
        event.id,
        event.type,
        JSON.stringify(event.actor || null),
        event.actor?.org || null,
        JSON.stringify({
          missionId: this.state.id.toString(),
          version: event.version,
          data: event.payload
        }),
        Date.now()
      )
      .run();
  }

  /* ================================ */

  async fetch(request) {
    const url = new URL(request.url);

    if (request.headers.get("Upgrade") === "websocket") {
      return this.handleConnection(request);
    }

    if (url.pathname === "/ingest/event" && request.method === "POST") {
      return this.handleEvent(await request.json());
    }

    if (url.pathname === "/system/refresh" && request.method === "POST") {
      return this.refreshProjection();
    }

    return new Response("Not found", { status: 404 });
  }

  validateEvent(event) {
    if (!event?.id || !event?.type || typeof event?.version !== "number")
      return "Missing id/type/version";

    if (!Object.values(MISSION_EVENTS).includes(event.type))
      return "Invalid event type";

    if (
      event.type === MISSION_EVENTS.STATUS_CHANGED &&
      !event.payload?.nextState
    )
      return "Missing nextState";

    return null;
  }

  async handleEvent(event) {
    const error = this.validateEvent(event);
    if (error) return json({ error }, 400);

    if (this.processedEvents.has(event.id))
      return json({ ok: true });

    if (event.version < this.missionCache.version)
      return json({ error: "Stale event version" }, 409);

    try {
      switch (event.type) {
        case MISSION_EVENTS.STATUS_CHANGED:
          await this.applyStatusChange(event);
          break;

        case MISSION_EVENTS.SLA_SCHEDULED:
          await this.scheduleTimer(
            event.payload.timerKey,
            event.payload.executeAt
          );
          break;

        case MISSION_EVENTS.SLA_CANCELLED:
          await this.cancelTimer(event.payload.timerKey);
          break;
      }

      // Authoritative durable audit append
      await this.appendAuditEvent(event);

      this.trackProcessedEvent(event.id);
      this.reportToFleet(event);
      this.broadcastRaw(event);

      return json({ ok: true });
    } catch (e) {
      return json({ error: e.message }, 403);
    }
  }

  async applyStatusChange(event) {
    const next = event.payload.nextState;
    const current = this.missionCache.status;

    if (!MISSION_FSM[current]?.includes(next)) {
      throw new Error(`Illegal FSM transition ${current} -> ${next}`);
    }

    const updated = {
      ...this.missionCache,
      status: next,
      version: event.version
    };

    this.missionCache = updated;
    await this.state.storage.put("missionCache", updated);

    if (TERMINAL_STATES.includes(next)) {
      this.timers.clear();
      await this.state.storage.delete("timers");
    }
  }

  trackProcessedEvent(eventId) {
    this.processedEvents.set(eventId, Date.now());
    this.eventCounter++;

    if (this.processedEvents.size > MAX_PROCESSED_EVENTS) {
      const oldest = [...this.processedEvents.entries()]
        .sort((a, b) => a[1] - b[1])[0][0];
      this.processedEvents.delete(oldest);
    }

    if (this.eventCounter >= BATCH_THRESHOLD) {
      this.eventCounter = 0;
      this.state.storage.put(
        "processedEvents",
        Array.from(this.processedEvents.entries())
      );
    }
  }

  reportToFleet(event) {
    const fleetId = this.env.FLEET_STATE.idFromName("global");

    this.env.FLEET_STATE
      .get(fleetId)
      .fetch("http://fleet/ingest", {
        method: "POST",
        body: JSON.stringify({
          missionId: this.state.id.toString(),
          type: event.type,
          payload: event.payload,
          version: event.version,
          timestamp: Date.now()
        })
      })
      .catch(() => {});
  }

  async scheduleTimer(key, executeAt) {
    this.timers.set(key, executeAt);

    await this.state.storage.put(
      "timers",
      Array.from(this.timers.entries())
    );

    await this.state.storage.setAlarm(Math.min(...this.timers.values()));
  }

  async cancelTimer(key) {
    this.timers.delete(key);
    await this.state.storage.put(
      "timers",
      Array.from(this.timers.entries())
    );
  }

  async alarm() {
    if (TERMINAL_STATES.includes(this.missionCache.status)) {
      this.timers.clear();
      await this.state.storage.delete("timers");
      return;
    }

    const now = Date.now();

    for (const [key, ts] of this.timers) {
      if (ts <= now) {
        this.timers.delete(key);
      }
    }

    await this.state.storage.put(
      "timers",
      Array.from(this.timers.entries())
    );

    if (this.timers.size > 0) {
      await this.state.storage.setAlarm(Math.min(...this.timers.values()));
    }
  }

  broadcastRaw(msg) {
    const data = JSON.stringify(msg);
    this.state.getWebSockets().forEach(ws => {
      try { ws.send(data); }
      catch { ws.close(); }
    });
  }

  async handleConnection(request) {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);

    return new Response(null, { status: 101, webSocket: client });
  }
}

/* =========================================================
   FLEET STATE
========================================================= */

export class FleetState {
  constructor(state) {
    this.state = state;
    this.globalIndex = new Map();

    state.blockConcurrencyWhile(async () => {
      const stored = await state.storage.get("globalIndex");
      if (stored) this.globalIndex = new Map(stored);
    });
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/ingest") {
      const data = await request.json();

      if (data.type === MISSION_EVENTS.STATUS_CHANGED) {
        this.globalIndex.set(data.missionId, {
          status: data.payload.nextState,
          version: data.version,
          lastSeen: Date.now()
        });

        await this.state.storage.put(
          "globalIndex",
          Array.from(this.globalIndex.entries())
        );
      }

      return new Response("OK");
    }

    if (url.pathname === "/dashboard") {
      return json(Array.from(this.globalIndex.entries()));
    }

    return new Response("Not found", { status: 404 });
  }
}

/* =========================================================
   DEFAULT EXPORT
========================================================= */

export default {
  async fetch() {
    return new Response("Mission Control Online");
  }
};
