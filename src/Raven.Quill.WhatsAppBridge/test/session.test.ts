import { afterEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pino } from "pino";
import { Session, SessionNotConnectedError, type WaSocket } from "../src/session.js";
import type { ClassifiedMessage } from "../src/messages.js";

const logger = pino({ level: "silent" });

class FakeSocket implements WaSocket {
    user: { id: string } | null = { id: "48123456789:5@s.whatsapp.net" };
    ended = false;
    loggedOut = false;
    sent: Array<{ jid: string; text: string }> = [];
    private readonly listeners = new Map<string, (arg: never) => void>();

    ev = {
        on: (event: string, listener: (arg: never) => void) => {
            this.listeners.set(event, listener);
        },
    };

    emit(event: string, arg: unknown): void {
        this.listeners.get(event)?.(arg as never);
    }

    async sendMessage(jid: string, content: { text: string }) {
        this.sent.push({ jid, text: content.text });
        return { key: { id: "SENT1" } };
    }

    async logout(): Promise<void> {
        this.loggedOut = true;
    }

    end(): void {
        this.ended = true;
    }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 10));

async function makeSession(inbound: ClassifiedMessage[] = []) {
    const authDir = await fs.mkdtemp(path.join(os.tmpdir(), "wa-session-test-"));
    const sockets: FakeSocket[] = [];
    const session = new Session(
        "db",
        "c".repeat(32),
        authDir,
        async () => {
            const socket = new FakeSocket();
            sockets.push(socket);
            return socket;
        },
        (message) => inbound.push(message),
        logger,
    );
    return { session, sockets, authDir, socket: () => sockets[sockets.length - 1]! };
}

let cleanup: Array<() => Promise<void> | void> = [];
afterEach(async () => {
    for (const fn of cleanup)
        await fn();
    cleanup = [];
});

describe("Session", () => {
    it("starts in starting state and moves to pairing on qr", async () => {
        const { session, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        assert.equal(session.status().state, "starting");

        socket().emit("connection.update", { qr: "QR-PAYLOAD-1" });
        await tick();

        const status = session.status();
        assert.equal(status.state, "pairing");
        assert.equal(status.qr, "QR-PAYLOAD-1");
        assert.ok(status.qrExpiresAt);
    });

    it("moves to connected with the phone number on open", async () => {
        const { session, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        socket().emit("connection.update", { qr: "QR" });
        socket().emit("connection.update", { connection: "open" });
        await tick();

        const status = session.status();
        assert.equal(status.state, "connected");
        assert.equal(status.phoneNumber, "+48123456789");
        assert.equal(status.qr, null);
    });

    it("wipes credentials and reports loggedOut on 401", async () => {
        const { session, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop());

        await session.start();
        socket().emit("connection.update", { connection: "open" });
        await tick();
        await fs.writeFile(path.join(authDir, "creds.json"), "{}");

        socket().emit("connection.update", {
            connection: "close",
            lastDisconnect: { error: { output: { statusCode: 401 } } },
        });
        await tick();

        const status = session.status();
        assert.equal(status.state, "loggedOut");
        assert.equal(status.phoneNumber, null);
        assert.ok(status.lastError);
        await assert.rejects(fs.access(authDir));
    });

    it("stops without reconnecting when another client replaces the session", async () => {
        const { session, sockets, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        socket().emit("connection.update", {
            connection: "close",
            lastDisconnect: { error: { output: { statusCode: 440 } } },
        });
        await tick();

        assert.equal(session.status().state, "disconnected");
        assert.match(session.status().lastError ?? "", /took over/);
        await tick();
        assert.equal(sockets.length, 1);
    });

    it("reconnects immediately when the server requires a restart after a scan", async () => {
        const { session, sockets, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        socket().emit("connection.update", { qr: "QR" });
        await tick();
        socket().emit("connection.update", {
            connection: "close",
            lastDisconnect: { error: { output: { statusCode: 515 } } },
        });
        await tick();

        assert.equal(sockets.length, 2);
        assert.equal(sockets[0]?.ended, true);
        assert.equal(session.status().state, "starting");
        assert.equal(session.status().lastError, null);
    });

    it("reports a pairing timeout without looping QR generation", async () => {
        const { session, sockets, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        socket().emit("connection.update", { qr: "QR" });
        await tick();
        socket().emit("connection.update", {
            connection: "close",
            lastDisconnect: { error: { output: { statusCode: 408 } } },
        });
        await tick();

        assert.equal(session.status().state, "disconnected");
        assert.match(session.status().lastError ?? "", /timed out/);
        assert.equal(sockets.length, 1);
    });

    it("forwards only notify upserts", async () => {
        const inbound: ClassifiedMessage[] = [];
        const { session, socket, authDir } = await makeSession(inbound);
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        const raw = {
            key: { remoteJid: "48123456789@s.whatsapp.net", fromMe: false, id: "M1" },
            message: { conversation: "hi" },
            messageTimestamp: 1,
        };
        socket().emit("messages.upsert", { type: "append", messages: [raw] });
        socket().emit("messages.upsert", { type: "notify", messages: [raw] });
        await tick();

        assert.equal(inbound.length, 1);
        assert.equal(inbound[0]?.text, "hi");
    });

    it("rejects sends while not connected and delivers when connected", async () => {
        const { session, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        await assert.rejects(session.sendText("48123456789@s.whatsapp.net", "x"), SessionNotConnectedError);

        socket().emit("connection.update", { connection: "open" });
        await tick();

        const messageId = await session.sendText("48123456789@s.whatsapp.net", "reply");
        assert.equal(messageId, "SENT1");
        assert.deepEqual(socket().sent, [{ jid: "48123456789@s.whatsapp.net", text: "reply" }]);
    });

    it("restart tears down the previous socket and starts a new one", async () => {
        const { session, sockets, authDir } = await makeSession();
        cleanup.push(() => session.stop(), () => fs.rm(authDir, { recursive: true, force: true }));

        await session.start();
        await session.restart();

        assert.equal(sockets.length, 2);
        assert.equal(sockets[0]?.ended, true);
        assert.equal(session.status().state, "starting");
    });

    it("delete logs out, tears down and wipes the auth dir", async () => {
        const { session, socket, authDir } = await makeSession();
        cleanup.push(() => session.stop());

        await session.start();
        socket().emit("connection.update", { connection: "open" });
        await tick();
        await fs.writeFile(path.join(authDir, "creds.json"), "{}");

        await session.delete();

        assert.equal(socket().loggedOut, true);
        assert.equal(socket().ended, true);
        await assert.rejects(fs.access(authDir));
    });
});
