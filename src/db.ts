import Database from "better-sqlite3";

// A single row in the 'samples' table, representing one data point from a device
export interface Sample {
    endpoint: string;
    // Device timestamps
    unix_time: number;
    millis: number | null;
    // Power
    battery_v: number | null;
    battery_pct: number | null;
    vbus_v: number | null;
    charging: 0 | 1;
    // Water
    water_temp: number | null;
    turbidity: number | null;
    tds: number | null;
    // Environment
    air_temp: number | null;
    humidity: number | null;
    air_velocity: number | null;
    ozone: number | null;
    uv: number | null;
    lum: number | null;
    baro: number | null;
    pm1_0: number | null;
    pm2_5: number | null;
    pm10: number | null;
    // Server receive time
    created_at: number;
}

// A single row in the 'commands' table, representing one command sent to a device
export interface Command {
    id: number;
    endpoint: string;
    cmd: string;
    payload: string | null;
    status: "pending" | "sent" | "acked";
    created_at: number;
    sent_at: number | null;
    ack_at: number | null;
}

// A single row in the 'sensor_health' table, representing the health status of a device's sensors
export interface SensorHealth {
    endpoint: string;
    unix_time: number;
    temperature: 0 | 1;
    turbidity: 0 | 1;
    tds: 0 | 1;
    environmental: 0 | 1;
    ozone: 0 | 1;
    air_velocity: 0 | 1;
    particulate_matter: 0 | 1;
    updated_at: number;
}

// A single device, identified by its endpoint and the timestamp of its most recent sample
export interface Device {
    endpoint: string;
    last_seen: number;
}

export interface EnqueueCommandArgs {
    endpoint: string;
    cmd: string;
    payload: string | null;
    createdAt: number;
}

export function createDb(dbPath: string) {
    const db = new Database(dbPath);

    // WAL mode improves write throughput and reduces writer blocking
    db.exec("PRAGMA journal_mode = WAL;");

    const insertSample = db.prepare<Sample>(`
        INSERT INTO samples (
            endpoint, unix_time, millis,
            battery_v, battery_pct, vbus_v, charging,
            water_temp, turbidity, tds,
            air_temp, humidity, air_velocity, ozone, uv, lum, baro,
            pm1_0, pm2_5, pm10,
            created_at
        ) VALUES (
            @endpoint, @unix_time, @millis,
            @battery_v, @battery_pct, @vbus_v, @charging,
            @water_temp, @turbidity, @tds,
            @air_temp, @humidity, @air_velocity, @ozone, @uv, @lum, @baro,
            @pm1_0, @pm2_5, @pm10,
            @created_at
        )
    `);

    const insertCommand = db.prepare(`
        INSERT INTO commands (endpoint, cmd, payload, status, created_at)
        VALUES (@endpoint, @cmd, @payload, 'pending', @created_at)
    `);

    const selectPendingCommand = db.prepare(`
        SELECT * FROM commands
        WHERE endpoint = ? AND status IN ('pending', 'sent')
        ORDER BY created_at ASC
        LIMIT 1
    `);

    const selectLatestSentCommand = db.prepare(`
        SELECT id FROM commands
        WHERE endpoint = ? AND status = 'sent'
        ORDER BY sent_at DESC
        LIMIT 1
    `);

    const markSent = db.prepare(`
        UPDATE commands SET status = 'sent', sent_at = ? WHERE id = ?
    `);

    const markAcked = db.prepare(`
        UPDATE commands SET status = 'acked', ack_at = ? WHERE id = ?
    `);

    // INSERT OR REPLACE overwrites the existing row for this endpoint (upsert by primary key)
    const upsertHealth = db.prepare<SensorHealth>(`
        INSERT OR REPLACE INTO sensor_health (
            endpoint, unix_time,
            temperature, turbidity, tds, environmental,
            ozone, air_velocity, particulate_matter,
            updated_at
        ) VALUES (
            @endpoint, @unix_time,
            @temperature, @turbidity, @tds, @environmental,
            @ozone, @air_velocity, @particulate_matter,
            @updated_at
        )
    `);

    const selectHealth = db.prepare(`
        SELECT * FROM sensor_health WHERE endpoint = ?
    `);

    const selectAllHealth = db.prepare(`
        SELECT * FROM sensor_health ORDER BY updated_at DESC
    `);

    const selectDevices = db.prepare(`
        SELECT endpoint, MAX(unix_time) AS last_seen
        FROM samples
        GROUP BY endpoint
        ORDER BY last_seen DESC
    `);

    // SQLite treats LIMIT -1 as "no limit",
    // which lets a single prepared statement serve both the limited and unlimited cases
    const selectSamples = db.prepare(`
        SELECT * FROM samples
        WHERE endpoint = ?
        ORDER BY unix_time DESC
        LIMIT ?
    `);

    const selectCommands = db.prepare(`
        SELECT * FROM commands
        WHERE endpoint = ?
        ORDER BY created_at DESC
        LIMIT ?
    `);

    const selectCommandsByStatus = db.prepare(`
        SELECT * FROM commands
        WHERE endpoint = ? AND status = ?
        ORDER BY created_at DESC
        LIMIT ?
    `);

    // "active" is a virtual status covering both pending and sent commands
    const selectActiveCommands = db.prepare(`
        SELECT * FROM commands
        WHERE endpoint = ? AND status IN ('pending', 'sent')
        ORDER BY created_at DESC
        LIMIT ?
    `);

    return {
        insertSample: (sample: Sample) => insertSample.run(sample),

        enqueueCommand: ({ endpoint, cmd, payload, createdAt }: EnqueueCommandArgs) =>
            insertCommand.run({ endpoint, cmd, payload, created_at: createdAt }),

        getPendingCommand: (endpoint: string): Command | undefined =>
            selectPendingCommand.get(endpoint) as Command | undefined,

        getLatestSentCommand: (endpoint: string): Pick<Command, "id"> | undefined =>
            selectLatestSentCommand.get(endpoint) as Pick<Command, "id"> | undefined,

        markSent: (id: number, sentAt: number) => markSent.run(sentAt, id),

        markAcked: (id: number, ackAt: number) => markAcked.run(ackAt, id),

        upsertHealth: (health: SensorHealth) => upsertHealth.run(health),

        getHealth: (endpoint: string): SensorHealth | undefined =>
            selectHealth.get(endpoint) as SensorHealth | undefined,

        listHealth: (): SensorHealth[] => selectAllHealth.all() as SensorHealth[],

        listDevices: (): Device[] => selectDevices.all() as Device[],

        // Pass null for limit to return all rows
        listSamples: (endpoint: string, limit: number | null): Sample[] =>
            selectSamples.all(endpoint, limit ?? -1) as Sample[],

        // Pass null for limit to return all rows
        // status: undefined = all, "active" = pending+sent, otherwise exact match
        listCommands: (endpoint: string, status: string | undefined, limit: number | null): Command[] => {
            const cap = limit ?? -1;
            if (status === "active") {
                return selectActiveCommands.all(endpoint, cap) as Command[];
            }
            if (status) {
                return selectCommandsByStatus.all(endpoint, status, cap) as Command[];
            }
            return selectCommands.all(endpoint, cap) as Command[];
        },
    };
}

export type Db = ReturnType<typeof createDb>;
