import { after, describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pino } from "pino";
import { buildServer } from "../src/http.js";
import { SessionManager } from "../src/session-manager.js";
import { InboundForwarder } from "../src/inbound-forwarder.js";
import type { WaSocket } from "../src/session.js";

const logger = pino({ level: "silent" });
const TOKEN = "test-bridge-token";
const CHANNEL_ID = "a".repeat(32);

class IdleSocket implements WaSocket {
    user = null;
    ev = { on: () => {} };
    async sendMessage() {
        return { key: { id: "X" } };
    }
    async requestPairingCode(): Promise<string> {
        return "ABCD1234";
    }
    async logout(): Promise<void> {}
    end(): void {}
}

async function makeApp() {
    const sessionsDir = await fs.mkdtemp(path.join(os.tmpdir(), "wa-http-test-"));
    const forwarder = new InboundForwarder("http://web", TOKEN, logger, [1]);
    const manager = new SessionManager(sessionsDir, async () => new IdleSocket(), forwarder, logger);
    const app = buildServer(manager, TOKEN, logger);
    return { app, manager, sessionsDir };
}

const authed = { "x-quill-bridge-token": TOKEN };

describe("bridge http", () => {
    it("rejects requests without the bridge token", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        const noToken = await app.inject({ method: "GET", url: `/sessions/db/${CHANNEL_ID}` });
        assert.equal(noToken.statusCode, 401);

        const badToken = await app.inject({
            method: "GET",
            url: `/sessions/db/${CHANNEL_ID}`,
            headers: { "x-quill-bridge-token": "wrong" },
        });
        assert.equal(badToken.statusCode, 401);
    });

    it("leaves healthz open", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        const response = await app.inject({ method: "GET", url: "/healthz" });
        assert.equal(response.statusCode, 200);
        assert.deepEqual(response.json(), { sessions: 0, connected: 0 });
    });

    it("rejects traversal-shaped path segments", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        for (const url of [
            `/sessions/..%2fescape/${CHANNEL_ID}`,
            `/sessions/.hidden/${CHANNEL_ID}`,
            `/sessions/db/${"Z".repeat(32)}`,
            `/sessions/db/short`,
        ]) {
            const response = await app.inject({ method: "POST", url, headers: authed });
            assert.equal(response.statusCode, 400, url);
        }
    });

    it("starts a session and reports its status", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        const missing = await app.inject({ method: "GET", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        assert.equal(missing.statusCode, 404);

        const started = await app.inject({ method: "POST", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        assert.equal(started.statusCode, 202);
        assert.equal(started.json().state, "starting");

        const status = await app.inject({ method: "GET", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        assert.equal(status.statusCode, 200);
        assert.equal(status.json().state, "starting");
    });

    it("accepts a phone number for pairing-code linking and rejects malformed ones", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        const accepted = await app.inject({
            method: "POST",
            url: `/sessions/db/${CHANNEL_ID}`,
            headers: authed,
            payload: { phoneNumber: "+48 123-456-789" },
        });
        assert.equal(accepted.statusCode, 202);

        for (const phoneNumber of ["12345", "not-a-number", 48123456789]) {
            const rejected = await app.inject({
                method: "POST",
                url: `/sessions/db/${CHANNEL_ID}/restart`,
                headers: authed,
                payload: { phoneNumber },
            });
            assert.equal(rejected.statusCode, 400, String(phoneNumber));
        }
    });

    it("returns 409 when sending on a session that is not connected", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        await app.inject({ method: "POST", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        const response = await app.inject({
            method: "POST",
            url: `/sessions/db/${CHANNEL_ID}/send`,
            headers: authed,
            payload: { to: "48123456789", text: "hi" },
        });
        assert.equal(response.statusCode, 409);
    });

    it("deletes sessions idempotently", async () => {
        const { app, manager, sessionsDir } = await makeApp();
        after(async () => {
            manager.shutdown();
            await app.close();
            await fs.rm(sessionsDir, { recursive: true, force: true });
        });

        await app.inject({ method: "POST", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        const first = await app.inject({ method: "DELETE", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        assert.equal(first.statusCode, 204);

        const second = await app.inject({ method: "DELETE", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        assert.equal(second.statusCode, 204);

        const status = await app.inject({ method: "GET", url: `/sessions/db/${CHANNEL_ID}`, headers: authed });
        assert.equal(status.statusCode, 404);
    });
});
