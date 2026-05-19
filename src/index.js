// =========================================================================
// UNIFIED MISSION CONTROL WORKER (ENTRYPOINT & STATEFUL DURABLE OBJECTS)
// =========================================================================

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

// Stateless Router Entrypoint
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      return new Response("Mission Control Online", { status: 200 });
    }

    // Route dashboard queries to FleetState global indexer
    if (url.pathname === "/dashboard") {
      const fleetId = env.FLEET_STATE.idFromName("global");
      const stub = env.FLEET_STATE.get(fleetId);
      return stub.fetch(request);
    }

    // Dynamic routing to specific MissionState Durable Object instance
    if (url.pathname.startsWith("/mission/")) {
      const missionId = url.searchParams.get("mission_id") || url.pathname.split("/")[2];
      if (!missionId) {
        return new Response(JSON.stringify({ error: "Missing mission_id parameter" }), {
          status: 400,
          headers: { "Content-Type": "application/json" }
        });
      }

      // Convert mission identification key to an DO ID
      const doId = env.MISSION_STATE.idFromName(missionId);
      const stub = env.MISSION_STATE.get(doId);
      
      // Pass request directly into stateful instance context
      return stub.fetch(request);
    }

    return new Response("Not Found", { status: 404 });
  }
};

/* =========================================================================
   DURABLE OBJECT: MISSION_STATE
   ========================================================================= */
export class MissionState {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.presence = new Map();
    this.timers = new Map();
    this.processedEvents = new Map();
    this.missionCache = null;

    // Load state from local storage before processing requests
    this.state.blockConcurrencyWhile(async () => {
      const [timers, events, cache] = await Promise.all([
        this.state.storage.get("timers"),
        this.state.storage.get("processedEvents"),
        this.state.storage.get("missionCache")
      ]);
      this.timers = new Map(timers || []);
      this.processedEvents = new Map(events || []);
      this.missionCache = cache || { status: "draft", version: 0 };
    });
  }

  async fetch(request) {
    const url = new URL(request.url);

    // Handle incoming WebSockets
    if (request.headers.get("Upgrade") === "websocket") {
      return this.handleConnection(request);
    }

    const pathSegments = url.pathname.split("/");
    // Matches patterns: /mission/:id/event, /mission/:id/state, etc.
    const action = pathSegments[3] || url.searchParams.get("action");

    // Enforce cryptographic JWT validation for administrative events
    let caller = { id: "system", email: "system@internal", role: "system" };
    if (request.method === "POST" && action === "event") {
      try {
        caller = await this.verifySupabaseAuth(request);
      } catch (authError) {
        return json({ error: `Authentication failed: ${authError.message}` }, 401);
      }
    }

    switch (action) {
      case "event":
        return this.handleEvent(await request.json(), caller);
      case "state":
        return json(this.missionCache);
      case "presence":
        return this.handlePresence(await request.json());
      default:
        return json({ error: "Unknown mission action endpoint", action }, 404);
    }
  }

  // --- CRYPTOGRAPHIC SUPABASE JWT VERIFICATION (HS256) ---
  async verifySupabaseAuth(request) {
    const authHeader = request.headers.get("Authorization");
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      throw new Error("Missing authorization header bearer token");
    }

    const token = authHeader.split(" ")[1];
    const jwtSecret = this.env.SUPABASE_JWT_SECRET;
    
    if (!jwtSecret) {
      throw new Error("SUPABASE_JWT_SECRET is not configured on the worker environment variables");
    }

    try {
      // 1. Split the token components
      const parts = token.split(".");
      if (parts.length !== 3) {
        throw new Error("Invalid JWT token structure");
      }

      const [headerB64, payloadB64, signatureB64] = parts;

      // 2. Cryptographically verify signature using Web Crypto HS256
      const isValid = await this.verifyHS256Signature(
        `${headerB64}.${payloadB64}`,
        signatureB64,
        jwtSecret
      );

      if (!isValid) {
        throw new Error("JWT signature verification failed");
      }

      // 3. Decode payload safely
      const payload = JSON.parse(this.base64UrlDecode(payloadB64));
      
      // 4. Check Expiration
      if (payload.exp && Date.now() / 1000 >= payload.exp) {
        throw new Error("Token has expired");
      }

      return {
        id: payload.sub,
        email: payload.email,
        role: payload.user_metadata?.role || "user",
        org: payload.user_metadata?.organization_id || null
      };
    } catch (e) {
      throw new Error(`Authentication validation failed: ${e.message}`);
    }
  }

  async verifyHS256Signature(data, signatureB64Url, secret) {
    const encoder = new TextEncoder();
    
    // Import the JWT Secret as a CryptoKey
    const key = await crypto.subtle.importKey(
      "raw",
      encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["verify"]
    );

    // Reconstruct the binary format of signature and signed data
    const signature = this.base64UrlToArrayBuffer(signatureB64Url);
    const dataBuffer = encoder.encode(data);

    // Cryptographic validation verify execution
    return await crypto.subtle.verify("HMAC", key, signature, dataBuffer);
  }

  // Helper helpers for parsing token components at the edge safely
  base64UrlToArrayBuffer(base64Url) {
    const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    const pad = base64.length % 4;
    const padded = pad ? base64 + "=".repeat(4 - pad) : base64;
    const binary = atob(padded);
    const buffer = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      buffer[i] = binary.charCodeAt(i);
    }
    return buffer.buffer;
  }

  base64UrlDecode(base64Url) {
    const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    return atob(base64);
  }

  // --- STATE HANDLERS ---
  async handleEvent(event, actor) {
    // Validate schema shape
    if (!event?.id || !event?.type || typeof event?.version !== "number") {
      return json({ error: "Missing identity metadata structure" }, 400);
    }

    // Deduplicate processed messages
    if (this.processedEvents.has(event.id)) {
      return json({ ok: true, deduplicated: true });
    }

    // Validate version sequences
    if (event.version < this.missionCache.version) {
      return json({ error: "Out of sync stale event version" }, 409);
    }

    try {
      switch (event.type) {
        case MISSION_EVENTS.STATUS_CHANGED:
          await this.applyStatusChange(event, actor);
          break;
        case MISSION_EVENTS.SLA_SCHEDULED:
          await this.scheduleTimer(event.payload.timerKey, event.payload.executeAt);
          break;
        case MISSION_EVENTS.SLA_CANCELLED:
          await this.cancelTimer(event.payload.timerKey);
          break;
      }

      // 1. Safe Transaction Write-Back directly to Supabase
      await this.syncToSupabase(event, actor);

      // 2. Forward updates to FleetState global indexer
      await this.reportToFleet(event);

      // 3. Offload downstream evaluations asynchronously to pivot and communication workers
      this.dispatchBackgroundPipelines(event);

      // Save processed cache
      this.processedEvents.set(event.id, Date.now());
      await this.state.storage.put("processedEvents", Array.from(this.processedEvents.entries()));

      // Broadcast changes on connected dashboard websockets
      this.broadcastRaw(event);

      return json({ ok: true });
    } catch (e) {
      return json({ error: e.message }, 403);
    }
  }

  async applyStatusChange(event, actor) {
    const next = event.payload.nextState;
    const current = this.missionCache.status;

    // RBAC policy check: Only super_admin can push to confirmed/completed directly
    if (["confirmed", "completed"].includes(next) && actor.role !== "super_admin" && actor.role !== "service_role") {
      throw new Error(`Unpermitted action: Role ${actor.role} cannot trigger status change to ${next}`);
    }

    // Validate state transitions
    if (!MISSION_FSM[current]?.includes(next)) {
      throw new Error(`Illegal state transition attempt from ${current} to ${next}`);
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

  // --- SYNC TO SUPABASE ---
  async syncToSupabase(event, actor) {
    const missionId = this.state.id.toString();
    const supabaseUrl = this.env.SUPABASE_URL;
    const supabaseKey = this.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!supabaseUrl || !supabaseKey) return;

    // Batch 1: Update main status schema
    if (event.type === MISSION_EVENTS.STATUS_CHANGED) {
      await fetch(`${supabaseUrl}/rest/v1/missions?id=eq.${missionId}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${supabaseKey}`,
          "apikey": supabaseKey,
          "Prefer": "resolution=merge-duplicates"
        },
        body: JSON.stringify({
          status: event.payload.nextState,
          version: event.version,
          updated_at: new Date().toISOString()
        })
      });
    }

    // Batch 2: Sync audits securely
    await fetch(`${supabaseUrl}/rest/v1/audit_logs`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${supabaseKey}`,
        "apikey": supabaseKey
      },
      body: JSON.stringify({
        mission_id: missionId,
        event_type: event.type,
        actor: actor,
        payload: event.payload,
        version: event.version
      })
    });
  }

  // --- UPDATE THE GLOBAL FLEET INDEX ---
  async reportToFleet(event) {
    if (!this.env.FLEET_STATE) return;
    const fleetId = this.env.FLEET_STATE.idFromName("global");
    await this.env.FLEET_STATE.get(fleetId).fetch("http://fleet/ingest", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        missionId: this.state.id.toString(),
        type: event.type,
        payload: event.payload,
        version: event.version,
        timestamp: Date.now()
      })
    }).catch(() => {});
  }

  // --- BACKGROUND TELEMETRY ---
  dispatchBackgroundPipelines(event) {
    const missionId = this.state.id.toString();

    // 1. Pivot Engine Check
    if (this.env["icc-pivot-engine"]) {
      this.env["icc-pivot-engine"].fetch("https://icc-pivot-engine/evaluate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mission_id: missionId, event_type: event.type })
      }).catch(() => {});
    }

    // 2. Notification Dispatcher
    if (this.env["mission-comms"]) {
      this.env["mission-comms"].fetch("https://mission-comms/event", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mission_id: missionId, event: event.type })
      }).catch(() => {});
    }
  }

  // --- WEBSOCKET BROADCAST SYSTEM ---
  async handleConnection(request) {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  broadcastRaw(msg) {
    const data = JSON.stringify(msg);
    this.state.getWebSockets().forEach((ws) => {
      try {
        ws.send(data);
      } catch {
        ws.close();
      }
    });
  }

  // --- SLA ALARMS AND TIMERS ---
  async scheduleTimer(key, executeAt) {
    this.timers.set(key, executeAt);
    await this.state.storage.put("timers", Array.from(this.timers.entries()));
    await this.state.storage.setAlarm(Math.min(...this.timers.values()));
  }

  async cancelTimer(key) {
    this.timers.delete(key);
    await this.state.storage.put("timers", Array.from(this.timers.entries()));
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
    await this.state.storage.put("timers", Array.from(this.timers.entries()));
    if (this.timers.size > 0) {
      await this.state.storage.setAlarm(Math.min(...this.timers.values()));
    }
  }
}

/* =========================================================================
   DURABLE OBJECT: FLEET_STATE
   ========================================================================= */
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

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}
