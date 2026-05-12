import mqtt, { MqttClient } from "mqtt";

export type ParsedJSON = Record<string, unknown>;

// MQTT config, an input to startMqtt
export interface MqttConfig {
    host: string;
    port: number;
    user?: string;
    pass?: string;
    tls: boolean;
    allowSelfSigned: boolean;
    onData: (endpoint: string, payload: ParsedJSON) => void;
    onInit: (endpoint: string, payload: ParsedJSON) => void;
    onAck: (endpoint: string) => void;
}

// Handle returned by startMqtt containing functions to interact with MQTT
export interface MqttHandle {
    client: MqttClient | null;
    publishControl: (endpoint: string, payload: string, options?: mqtt.IClientPublishOptions) => void;
    clearControl: (endpoint: string) => void;
}

// Parse MQTT message payload as JSON, returning null if parsing fails.
function parseJson(message: Buffer): ParsedJSON | null {
    try {
        return JSON.parse(message.toString()) as ParsedJSON;
    } catch {
        return null;
    }
}

export function startMqtt(config: MqttConfig): MqttHandle {
    const { host, port, user, pass, tls, allowSelfSigned, onData, onInit, onAck } = config;
    if (!host) {
        console.warn("MQTT_HOST not set; MQTT is disabled");
        return {
            client: null,
            publishControl: () => {},
            clearControl: () => {},
        };
    }

    // Form MQTT connection options and connect to the broker based on the provided configuration
    const options: mqtt.IClientOptions = {
        username: user,
        password: pass,
        reconnectPeriod: 2000,
        keepalive: 60,
        // When using TLS, optionally relax certificate checks for self-signed certs
        ...(tls && { rejectUnauthorized: !allowSelfSigned }),
    };
    const client = mqtt.connect(`${tls ? "mqtts" : "mqtt"}://${host}:${port}`, options);

    client.on("connect", () => {
        // Subscribe to data and ack topics for all endpoints on connection to receive updates from devices
        client.subscribe("minimote/+/data", { qos: 0 });
        client.subscribe("minimote/+/init", { qos: 0 });
        client.subscribe("minimote/+/ack", { qos: 0 });
        console.log(`MQTT connected to ${host}:${port}`);
    });

    client.on("error", err => {
        console.error("MQTT error:", err.message);
    });

    client.on("message", (topic: string, message: Buffer) => {
        // Topic format: minimote/<endpoint>/<type>
        const parts = topic.split("/");
        if (parts.length < 3) {
            return;
        }
        // Extract endpoint and message type from the topic
        const [, endpoint, type] = parts;
        const payload = parseJson(message);
        if (!payload) {
            return;
        }

        console.log(`Processing ${type} message from ${endpoint}`);
        if (type === "data") {
            onData(endpoint, payload);
        } else if (type === "init") {
            onInit(endpoint, payload);
        } else if (type === "ack") {
            onAck(endpoint);
        } else {
            console.warn(`Unhandled MQTT topic type: ${type}`);
        }
    });

    return {
        client,

        // Publish a control message to a specific endpoint.
        // The message is retained until `clearControl` is called on that same endpoint.
        publishControl: (endpoint, payload) => {
            client.publish(`minimote/${endpoint}/control`, payload, {
                // Deliver with QoS 1 and retain so that the device receives the command even if it was offline when the command was sent
                // It will be retained until the device acks the command
                qos: 1,
                retain: true,
            });
        },

        // Clear a retained control message.
        clearControl: endpoint => {
            // Message is null to clear the retained message
            client.publish(`minimote/${endpoint}/control`, "", {
                qos: 1,
                retain: true,
            });
        },
    };
}
