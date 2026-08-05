import fs from "node:fs/promises";
import path from "node:path";
import type { Logger } from "./logger.js";
import type { InboundForwarder } from "./inbound-forwarder.js";
import { Session, type SocketFactory } from "./session.js";

export class SessionManager {
    private readonly sessions = new Map<string, Session>();

    constructor(
        private readonly sessionsDir: string,
        private readonly socketFactory: SocketFactory,
        private readonly forwarder: InboundForwarder,
        private readonly logger: Logger,
    ) {}

    get(database: string, channelId: string): Session | undefined {
        return this.sessions.get(keyOf(database, channelId));
    }

    async start(database: string, channelId: string): Promise<Session> {
        const existing = this.get(database, channelId);
        if (existing !== undefined)
            return existing;

        const authDir = this.authDirOf(database, channelId);
        await fs.mkdir(authDir, { recursive: true, mode: 0o700 });

        const session = new Session(
            database,
            channelId,
            authDir,
            this.socketFactory,
            (message) =>
                void this.forwarder.forward({
                    database,
                    channelId,
                    sender: message.sender,
                    messageId: message.messageId,
                    kind: message.kind,
                    text: message.text,
                    timestamp: message.timestamp,
                }),
            this.logger,
        );

        this.sessions.set(keyOf(database, channelId), session);
        await session.start();
        return session;
    }

    async restart(database: string, channelId: string): Promise<Session> {
        const existing = this.get(database, channelId);
        if (existing === undefined)
            return this.start(database, channelId);

        await existing.restart();
        return existing;
    }

    async delete(database: string, channelId: string): Promise<void> {
        const existing = this.get(database, channelId);
        if (existing !== undefined) {
            this.sessions.delete(keyOf(database, channelId));
            await existing.delete();
            return;
        }

        // No live session: still wipe any credentials left on disk.
        await fs.rm(this.authDirOf(database, channelId), { recursive: true, force: true });
    }

    // Linked sessions leave creds.json behind; resume them so a bridge restart
    // does not silence connected channels. Never-paired sessions leave nothing.
    async resumeFromDisk(): Promise<void> {
        let databases: string[];
        try {
            databases = await fs.readdir(this.sessionsDir);
        } catch {
            return;
        }

        for (const database of databases) {
            let channelIds: string[];
            try {
                channelIds = await fs.readdir(path.join(this.sessionsDir, database));
            } catch {
                continue;
            }

            for (const channelId of channelIds) {
                try {
                    await fs.access(path.join(this.sessionsDir, database, channelId, "creds.json"));
                } catch {
                    continue;
                }

                try {
                    await this.start(database, channelId);
                    this.logger.info({ database, channelId }, "resumed whatsapp session from disk");
                } catch (error) {
                    // One broken session must not stop the rest from resuming.
                    this.logger.warn(
                        { database, channelId, error: String(error) },
                        "failed to resume whatsapp session",
                    );
                }
            }
        }
    }

    counts(): { sessions: number; connected: number } {
        let connected = 0;
        for (const session of this.sessions.values()) {
            if (session.status().state === "connected")
                connected++;
        }
        return { sessions: this.sessions.size, connected };
    }

    shutdown(): void {
        for (const session of this.sessions.values())
            session.stop();
        this.sessions.clear();
    }

    private authDirOf(database: string, channelId: string): string {
        return path.join(this.sessionsDir, database, channelId);
    }
}

function keyOf(database: string, channelId: string): string {
    return `${database}/${channelId}`;
}
