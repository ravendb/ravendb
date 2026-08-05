import fs from "node:fs/promises";
import type { Logger } from "./logger.js";
import { classifyMessage, type ClassifiedMessage } from "./messages.js";

export type SessionState = "starting" | "pairing" | "connected" | "disconnected" | "loggedOut";

export interface SessionStatus {
    state: SessionState;
    qr: string | null;
    qrExpiresAt: string | null;
    phoneNumber: string | null;
    lastError: string | null;
}

export interface ConnectionUpdate {
    connection?: string;
    qr?: string;
    lastDisconnect?: { error?: unknown };
}

export interface MessagesUpsert {
    type: string;
    messages: unknown[];
}

// The narrow surface of a Baileys socket the session needs; tests inject fakes.
export interface WaSocket {
    user?: { id: string } | null;
    ev: { on(event: string, listener: (arg: never) => void): void };
    sendMessage(jid: string, content: { text: string }): Promise<{ key?: { id?: string | null } } | undefined>;
    logout(): Promise<void>;
    end(error?: Error): void;
}

export type SocketFactory = (authDir: string) => Promise<WaSocket>;

export class SessionNotConnectedError extends Error {
    constructor() {
        super("session is not connected");
    }
}

// Baileys DisconnectReason values (Boom statusCode on connection close).
const LOGGED_OUT = 401;
const TIMED_OUT = 408;
const CONNECTION_REPLACED = 440;

const QR_TTL_MS = 60_000;
const MIN_RECONNECT_MS = 1_000;
const MAX_RECONNECT_MS = 60_000;

export class Session {
    private socket: WaSocket | null = null;
    private state: SessionState = "starting";
    private qr: string | null = null;
    private qrExpiresAt: Date | null = null;
    private phoneNumber: string | null = null;
    private lastError: string | null = null;
    private stopped = false;
    private reconnectDelayMs = MIN_RECONNECT_MS;
    private reconnectTimer: NodeJS.Timeout | null = null;
    // Bumped on every teardown so events from a superseded socket are ignored.
    private generation = 0;

    constructor(
        readonly database: string,
        readonly channelId: string,
        private readonly authDir: string,
        private readonly socketFactory: SocketFactory,
        private readonly onInbound: (message: ClassifiedMessage) => void,
        private readonly logger: Logger,
    ) {}

    status(): SessionStatus {
        return {
            state: this.state,
            qr: this.state === "pairing" ? this.qr : null,
            qrExpiresAt: this.state === "pairing" && this.qrExpiresAt ? this.qrExpiresAt.toISOString() : null,
            phoneNumber: this.phoneNumber,
            lastError: this.lastError,
        };
    }

    async start(): Promise<void> {
        this.stopped = false;
        this.teardownSocket();
        const generation = this.generation;
        this.state = "starting";
        this.qr = null;

        let socket: WaSocket;
        try {
            socket = await this.socketFactory(this.authDir);
        } catch (error) {
            if (generation !== this.generation)
                return;
            this.lastError = String(error instanceof Error ? error.message : error);
            this.state = "disconnected";
            this.scheduleReconnect();
            return;
        }

        if (generation !== this.generation) {
            try {
                socket.end();
            } catch {
                // superseded before it was adopted; nothing to clean up
            }
            return;
        }

        this.socket = socket;
        socket.ev.on("connection.update", ((update: ConnectionUpdate) => {
            if (generation === this.generation)
                void this.onConnectionUpdate(update);
        }) as never);
        socket.ev.on("messages.upsert", ((upsert: MessagesUpsert) => {
            if (generation === this.generation)
                this.onMessagesUpsert(upsert);
        }) as never);
    }

    async restart(): Promise<void> {
        this.teardownSocket();
        if (this.state === "loggedOut")
            await this.wipeAuthDir();
        this.lastError = null;
        this.phoneNumber = null;
        await this.start();
    }

    async sendText(toJid: string, text: string): Promise<string> {
        const socket = this.socket;
        if (this.state !== "connected" || socket === null)
            throw new SessionNotConnectedError();

        const result = await socket.sendMessage(toJid, { text });
        return result?.key?.id ?? "";
    }

    async delete(): Promise<void> {
        this.stopped = true;
        const socket = this.socket;
        if (socket !== null) {
            try {
                // Best effort: tells WhatsApp to unlink the device so the phone shows it gone.
                await Promise.race([socket.logout(), new Promise((resolve) => setTimeout(resolve, 5_000))]);
            } catch {
                // a dead socket must not block credential wipe
            }
        }
        this.teardownSocket();
        await this.wipeAuthDir();
    }

    stop(): void {
        this.stopped = true;
        this.teardownSocket();
    }

    private async onConnectionUpdate(update: ConnectionUpdate): Promise<void> {
        if (update.qr) {
            this.state = "pairing";
            this.qr = update.qr;
            this.qrExpiresAt = new Date(Date.now() + QR_TTL_MS);
            return;
        }

        if (update.connection === "open") {
            this.state = "connected";
            this.qr = null;
            this.lastError = null;
            this.reconnectDelayMs = MIN_RECONNECT_MS;
            this.phoneNumber = extractPhoneNumber(this.socket?.user?.id);
            this.logger.info(
                { database: this.database, channelId: this.channelId },
                "whatsapp session connected",
            );
            return;
        }

        if (update.connection !== "close")
            return;

        const wasPairing = this.state === "pairing";
        const statusCode = disconnectStatusCode(update.lastDisconnect?.error);

        if (statusCode === LOGGED_OUT) {
            this.teardownSocket();
            await this.wipeAuthDir();
            this.state = "loggedOut";
            this.qr = null;
            this.phoneNumber = null;
            this.lastError = "the phone unlinked this device";
            return;
        }

        if (statusCode === CONNECTION_REPLACED) {
            this.teardownSocket();
            this.state = "disconnected";
            this.qr = null;
            this.lastError = "another client took over this WhatsApp session";
            return;
        }

        if (statusCode === TIMED_OUT && wasPairing) {
            // WhatsApp closes the socket once the QR budget is spent; do not loop QR
            // generation (ban hygiene) - the dashboard restart issues a fresh code.
            this.teardownSocket();
            this.state = "disconnected";
            this.qr = null;
            this.lastError = "pairing timed out - generate a new code";
            return;
        }

        this.state = "disconnected";
        this.qr = null;
        this.lastError = disconnectMessage(update.lastDisconnect?.error);
        if (!this.stopped)
            this.scheduleReconnect();
    }

    private onMessagesUpsert(upsert: MessagesUpsert): void {
        // "notify" is live traffic; "append" replays history/offline backlog after a
        // relink and must not flood the agent.
        if (upsert.type !== "notify")
            return;

        for (const raw of upsert.messages) {
            try {
                const classified = classifyMessage(raw as never);
                if (classified !== null)
                    this.onInbound(classified);
            } catch (error) {
                this.logger.warn(
                    { database: this.database, channelId: this.channelId, error: String(error) },
                    "failed to classify inbound message",
                );
            }
        }
    }

    private scheduleReconnect(): void {
        if (this.reconnectTimer !== null)
            return;

        const delay = this.reconnectDelayMs + Math.floor(Math.random() * 250);
        this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, MAX_RECONNECT_MS);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            void this.start();
        }, delay);
        this.reconnectTimer.unref?.();
    }

    private teardownSocket(): void {
        this.generation++;
        if (this.reconnectTimer !== null) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        const socket = this.socket;
        this.socket = null;
        if (socket !== null) {
            try {
                socket.end();
            } catch {
                // closing an already-dead socket is fine
            }
        }
    }

    private async wipeAuthDir(): Promise<void> {
        await fs.rm(this.authDir, { recursive: true, force: true });
    }
}

function extractPhoneNumber(userId: string | null | undefined): string | null {
    if (!userId)
        return null;
    const digits = userId.split("@")[0]?.split(":")[0] ?? "";
    return digits.length > 0 ? `+${digits}` : null;
}

function disconnectStatusCode(error: unknown): number | null {
    const statusCode = (error as { output?: { statusCode?: unknown } } | undefined)?.output?.statusCode;
    return typeof statusCode === "number" ? statusCode : null;
}

function disconnectMessage(error: unknown): string {
    if (error instanceof Error)
        return error.message;
    return error === undefined || error === null ? "connection closed" : String(error);
}
