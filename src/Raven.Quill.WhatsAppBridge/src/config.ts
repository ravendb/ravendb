import path from "node:path";

export interface BridgeConfig {
    host: string;
    port: number;
    dataDir: string;
    sessionsDir: string;
    tokenPath: string;
    webInternalUrl: string;
    logLevel: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): BridgeConfig {
    const listen = env.RAVEN_QUILL_WHATSAPP_BRIDGE_LISTEN ?? "127.0.0.1:8447";
    const separator = listen.lastIndexOf(":");
    if (separator <= 0)
        throw new Error(`RAVEN_QUILL_WHATSAPP_BRIDGE_LISTEN must be host:port, got '${listen}'`);

    const host = listen.slice(0, separator);
    const port = Number(listen.slice(separator + 1));
    if (!Number.isInteger(port) || port <= 0 || port > 65535)
        throw new Error(`RAVEN_QUILL_WHATSAPP_BRIDGE_LISTEN has an invalid port in '${listen}'`);

    const dataDir = env.RAVEN_QUILL_WHATSAPP_DATA_DIR ?? "/var/lib/quill/whatsapp";

    return {
        host,
        port,
        dataDir,
        sessionsDir: path.join(dataDir, "sessions"),
        tokenPath: path.join(dataDir, "bridge-token"),
        webInternalUrl: (env.RAVEN_QUILL_WEB_INTERNAL_URL ?? "http://127.0.0.1:5000").replace(/\/+$/, ""),
        logLevel: env.RAVEN_QUILL_WHATSAPP_BRIDGE_LOG_LEVEL ?? "info",
    };
}
