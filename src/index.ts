import "dotenv/config";
import bcrypt from "bcrypt";
import express, { Request, Response } from "express";
import rateLimit from "express-rate-limit";
import jwt, { JwtPayload } from "jsonwebtoken";
import { createDb, Sample, SensorHealth } from "./db";
import { startMqtt, ParsedJSON } from "./mqtt";

// Configuration derived from .env with defaults
const config = {
    httpPort: parseInt(process.env.HTTP_PORT ?? "3000", 10),
    dbPath: process.env.DB_PATH ?? "./minimote.db",
    adminPasswordHash: process.env.ADMIN_PASSWORD_HASH ?? "",
    jwtSecret: process.env.JWT_SECRET ?? "",
    mqtt: {
        host: process.env.MQTT_HOST ?? "",
        port: parseInt(process.env.MQTT_PORT ?? "8883", 10),
        user: process.env.MQTT_USER,
        pass: process.env.MQTT_PASS,
        tls: (process.env.MQTT_TLS ?? "true") !== "false",
        allowSelfSigned: process.env.MQTT_ALLOW_SELF_SIGNED === "true",
    },
};

/* Auth */

function requireAuth(req: Request, res: Response, next: express.NextFunction): void {
    const header = req.headers.authorization;
    if (!header?.startsWith("Bearer ")) {
        res.status(401).json({ error: "missing token" });
        return;
    }
    try {
        jwt.verify(header.slice(7), config.jwtSecret) as JwtPayload;
        next();
    } catch {
        res.status(401).json({ error: "invalid or expired token" });
    }
}

const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 10, // 10 attempts per IP
    standardHeaders: true,
    legacyHeaders: false,
    message: {
        error: "too many login attempts, try again later",
    },
});

/* Helpers */

// Returns current time as a Unix timestamp
function unixNow(): number {
    return Math.floor(Date.now() / 1000);
}

/**
 * Parse a query-string limit value.
 * @param raw starting string from query parameter
 * @param defaultValue default value to return if raw is undefined or invalid
 * @param maxValue optional maximum value to cap the result at
 * @returns parsed integer limit, or null for 'all'/'0', or defaultValue if invalid/undefined
 */
function parseLimit(raw: string | undefined, defaultValue: number, maxValue?: number): number | null {
    if (!raw) {
        return defaultValue;
    }
    if (raw === "all" || raw === "0") {
        return null;
    }

    const parsed = parseInt(raw, 10);
    if (Number.isNaN(parsed)) {
        return defaultValue;
    }
    return maxValue ? Math.min(parsed, maxValue) : parsed;
}

/**
 * Map a raw MQTT data payload to a normalized Sample row.
 * @param endpoint MQTT topic endpoint the data came from
 * @param payload parsed JSON object from the MQTT message
 * @returns normalized sample ready for database insertion
 */
function normalizeSample(endpoint: string, payload: ParsedJSON) {
    return {
        endpoint,
        ...payload,
        charging: payload.charging ? 1 : 0, // Convert from JSON boolean to database integer
        unix_time: payload.unix_time ?? unixNow(),
        created_at: unixNow(),
    } as Sample;
}

/**
 * Map a raw MQTT health payload to a normalized SensorHealth row.
 * @param endpoint MQTT topic endpoint the health data came from
 * @param payload parsed JSON object from the MQTT message
 * @returns normalized sensor health ready for database insertion
 */
function normalizeSensorHealth(endpoint: string, payload: ParsedJSON) {
    return {
        endpoint,
        unix_time: payload.unix_time ?? unixNow(),
        temperature: payload["temperature sensor"] ? 1 : 0,
        turbidity: payload["turbidity sensor"] ? 1 : 0,
        tds: payload["TDS sensor"] ? 1 : 0,
        environmental: payload["environmental sensors"] ? 1 : 0,
        ozone: payload["ozone sensor"] ? 1 : 0,
        air_velocity: payload["air velocity sensor"] ? 1 : 0,
        particulate_matter: payload["particulate matter sensor"] ? 1 : 0,
        updated_at: unixNow(),
    } as SensorHealth;
}

/**
 * Immediately publish a command to the device via MQTT.
 * @param endpoint device endpoint to publish to
 * @param payload payload string to send
 * @param commandId optional database command ID to mark as sent, or null to skip db update
 * @returns true if published successfully
 */
function publishCommandNow(endpoint: string, payload: string, commandId: number | null): boolean {
    if (!mqtt?.client) {
        return false;
    }
    mqtt.publishControl(endpoint, payload);
    if (commandId !== null) {
        db.markSent(commandId, unixNow());
    }
    return true;
}

/* Backend setup */

// Connect to database and MQTT broker, and start Express server
const db = createDb(config.dbPath);
const mqtt = startMqtt({
    ...config.mqtt,
    // When we receive data from a device, insert it into the database as a new sample
    onData: (endpoint, payload) => {
        db.insertSample(normalizeSample(endpoint, payload));
    },
    // When we receive health data from a device, upsert it into the database
    onInit: (endpoint, payload) => {
        db.upsertHealth(normalizeSensorHealth(endpoint, payload));
    },
    // When we receive a command ack from a device, mark the latest sent command as acknowledged
    onAck: endpoint => {
        if (!endpoint) {
            return;
        }
        const row = db.getLatestSentCommand(endpoint);
        if (!row) {
            return;
        }
        db.markAcked(row.id, unixNow());
        mqtt?.clearControl(endpoint); // Clear the command from MQTT so the device doesn't receive it again
    },
});
const app = express();
app.use(express.json());
app.enable("trust proxy");

/* API endpoints */

app.get("/api/devices", (_req: Request, res: Response) => {
    res.json(db.listDevices());
});

// GET /api/health?endpoint=<id>  -> single device
// GET /api/health                -> all devices
app.get("/api/health", requireAuth, (req: Request, res: Response) => {
    const { endpoint } = req.query as Record<string, string | undefined>;
    if (endpoint) {
        const health = db.getHealth(endpoint);
        if (!health) {
            return res.status(404).json({ error: "no health report for endpoint" });
        }
        return res.json(health);
    }
    res.json(db.listHealth());
});

// GET /api/samples?endpoint=<id>&limit=<n|all>  -> samples for a device, limited to n or all
app.get("/api/samples", (req: Request, res: Response) => {
    const { endpoint, limit: rawLimit } = req.query as Record<string, string | undefined>;
    if (!endpoint) {
        return res.status(400).json({ error: "endpoint required" });
    }

    const limit = parseLimit(rawLimit, 100, 5000);
    res.json(db.listSamples(endpoint, limit));
});

// GET /api/commands?endpoint=<id>&status=<sent|acked|all>&limit=<n|all>  -> commands for a device, filtered by status and limited to n or all
app.get("/api/commands", requireAuth, (req: Request, res: Response) => {
    const { endpoint, status, limit: rawLimit } = req.query as Record<string, string | undefined>;
    if (!endpoint) {
        return res.status(400).json({ error: "endpoint required" });
    }

    const limit = parseLimit(rawLimit, 50, 1000);
    res.json(db.listCommands(endpoint, status, limit));
});

// POST /api/commands  -> send a command to a device, with optional payload
app.post("/api/commands", requireAuth, (req: Request, res: Response) => {
    const { endpoint, cmd, payload } = (req.body ?? {}) as {
        endpoint?: string;
        cmd?: string;
        payload?: unknown;
    };

    if (!endpoint || !cmd) {
        return res.status(400).json({ error: "endpoint and cmd required" });
    }

    // Serialise payload to a string, or null if absent
    const storedPayload: string | null =
        payload == null ? null : typeof payload === "string" ? payload : JSON.stringify(payload);

    const result = db.enqueueCommand({ endpoint, cmd, payload: storedPayload, createdAt: unixNow() });
    const commandId = (result?.lastInsertRowid as number) ?? null;

    // If no payload, wrap the command name so the device still gets valid JSON
    const publishPayload = storedPayload ?? JSON.stringify({ cmd });
    const sent = publishCommandNow(endpoint, publishPayload, commandId);

    res.json({ ok: true, sent });
});

app.post("/api/auth/login", loginLimiter, async (req: Request, res: Response) => {
    const { password } = req.body ?? {};
    if (!password) {
        return res.status(401).json({ error: "invalid credentials" });
    }

    const valid = await bcrypt.compare(password, config.adminPasswordHash);
    if (!valid) {
        return res.status(401).json({ error: "invalid credentials" });
    }

    const token = jwt.sign({ role: "admin" }, config.jwtSecret, { expiresIn: "8h" });
    res.json({ token });
});

app.post("/api/auth/logout", (_req: Request, res: Response) => {
    // Logout is handled client-side by discarding the token
    res.json({ ok: true });
});

app.listen(config.httpPort, "127.0.0.1", () => {
    console.log(`HTTP listening on 127.0.0.1:${config.httpPort}`);
});
