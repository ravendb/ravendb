import fs from "node:fs/promises";
import { loadConfig } from "./config.js";
import { createLogger, type Logger } from "./logger.js";
import { InboundForwarder } from "./inbound-forwarder.js";
import { SessionManager } from "./session-manager.js";
import { baileysSocketFactory } from "./baileys.js";
import { buildServer } from "./http.js";

const config = loadConfig();
const logger = createLogger(config.logLevel);

await fs.mkdir(config.sessionsDir, { recursive: true });

const token = await waitForToken(config.tokenPath, logger);

const forwarder = new InboundForwarder(config.webInternalUrl, token, logger);
const manager = new SessionManager(config.sessionsDir, baileysSocketFactory(logger), forwarder, logger);
await manager.resumeFromDisk();

const app = buildServer(manager, token, logger);

for (const signal of ["SIGTERM", "SIGINT"] as const) {
    process.on(signal, () => {
        logger.info({ signal }, "shutting down");
        manager.shutdown();
        void app.close().then(() => process.exit(0));
    });
}

await app.listen({ host: config.host, port: config.port });
logger.info({ host: config.host, port: config.port, sessionsDir: config.sessionsDir }, "whatsapp bridge listening");

async function waitForToken(tokenPath: string, log: Logger): Promise<string> {
    for (let attempt = 1; ; attempt++) {
        try {
            const token = (await fs.readFile(tokenPath, "utf8")).trim();
            if (token.length > 0)
                return token;
        } catch {
            // not there yet
        }

        if (attempt % 10 === 0)
            log.info({ tokenPath, attempt }, "waiting for bridge token file");
        await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
}
