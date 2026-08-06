import { createHash, timingSafeEqual } from "node:crypto";
import fastify, { type FastifyInstance } from "fastify";
import type { Logger } from "./logger.js";
import type { SessionManager } from "./session-manager.js";
import { SessionNotConnectedError } from "./session.js";

// Both segments become directory names under the sessions dir: the strict
// shapes double as the path-traversal guard.
const DATABASE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const CHANNEL_ID_PATTERN = /^[a-f0-9]{32}$/;

const MAX_TEXT_LENGTH = 60_000;

interface SessionParams {
    database: string;
    channelId: string;
}

interface PairingBody {
    phoneNumber?: unknown;
}

/// Returns the digits of an E.164-ish number, null when absent, undefined when malformed.
function readPairingPhoneNumber(body: PairingBody | undefined): string | null | undefined {
    const raw = body?.phoneNumber;
    if (raw === undefined || raw === null || raw === "")
        return null;

    if (typeof raw !== "string")
        return undefined;

    const digits = raw.replace(/[\s()+-]/g, "");
    return /^[0-9]{6,20}$/.test(digits) ? digits : undefined;
}

export function buildServer(manager: SessionManager, token: string, logger: Logger): FastifyInstance {
    const app = fastify({ logger: false });
    const tokenDigest = createHash("sha256").update(token, "utf8").digest();

    app.addHook("onRequest", async (request, reply) => {
        if (request.url === "/healthz")
            return;

        const provided = request.headers["x-quill-bridge-token"];
        if (typeof provided !== "string" || !digestsMatch(tokenDigest, provided)) {
            logger.warn({ url: request.url }, "rejected request with missing or invalid bridge token");
            return reply.code(401).send({ error: "invalid bridge token" });
        }
    });

    app.get("/healthz", async () => manager.counts());

    app.post<{ Params: SessionParams; Body: PairingBody }>("/sessions/:database/:channelId", async (request, reply) => {
        const params = validateParams(request.params);
        if (params === null)
            return reply.code(400).send({ error: "invalid database or channelId" });

        const phoneNumber = readPairingPhoneNumber(request.body);
        if (phoneNumber === undefined)
            return reply.code(400).send({ error: "phoneNumber must be 6-20 digits" });

        const session = await manager.start(params.database, params.channelId, phoneNumber);
        return reply.code(202).send({ state: session.status().state });
    });

    app.get<{ Params: SessionParams }>("/sessions/:database/:channelId", async (request, reply) => {
        const params = validateParams(request.params);
        if (params === null)
            return reply.code(400).send({ error: "invalid database or channelId" });

        const session = manager.get(params.database, params.channelId);
        if (session === undefined)
            return reply.code(404).send({ error: "unknown session" });

        return reply.send(session.status());
    });

    app.post<{ Params: SessionParams; Body: PairingBody }>(
        "/sessions/:database/:channelId/restart",
        async (request, reply) => {
            const params = validateParams(request.params);
            if (params === null)
                return reply.code(400).send({ error: "invalid database or channelId" });

            const phoneNumber = readPairingPhoneNumber(request.body);
            if (phoneNumber === undefined)
                return reply.code(400).send({ error: "phoneNumber must be 6-20 digits" });

            const session = await manager.restart(params.database, params.channelId, phoneNumber);
            return reply.code(202).send({ state: session.status().state });
        },
    );

    app.post<{ Params: SessionParams; Body: { to?: unknown; text?: unknown } }>(
        "/sessions/:database/:channelId/send",
        async (request, reply) => {
            const params = validateParams(request.params);
            if (params === null)
                return reply.code(400).send({ error: "invalid database or channelId" });

            const session = manager.get(params.database, params.channelId);
            if (session === undefined)
                return reply.code(404).send({ error: "unknown session" });

            const to = request.body?.to;
            const text = request.body?.text;
            if (typeof to !== "string" || to.length === 0 || typeof text !== "string" || text.length === 0)
                return reply.code(400).send({ error: "to and text are required" });
            if (text.length > MAX_TEXT_LENGTH)
                return reply.code(400).send({ error: `text exceeds ${MAX_TEXT_LENGTH} chars` });

            try {
                const messageId = await session.sendText(normalizeJid(to), text);
                return reply.send({ messageId });
            } catch (error) {
                if (error instanceof SessionNotConnectedError)
                    return reply.code(409).send({ error: "session is not connected" });
                throw error;
            }
        },
    );

    app.delete<{ Params: SessionParams }>("/sessions/:database/:channelId", async (request, reply) => {
        const params = validateParams(request.params);
        if (params === null)
            return reply.code(400).send({ error: "invalid database or channelId" });

        await manager.delete(params.database, params.channelId);
        return reply.code(204).send();
    });

    return app;
}

function validateParams(params: SessionParams): SessionParams | null {
    return DATABASE_PATTERN.test(params.database) && CHANNEL_ID_PATTERN.test(params.channelId)
        ? params
        : null;
}

function normalizeJid(to: string): string {
    return to.includes("@") ? to : `${to.replace(/^\+/, "")}@s.whatsapp.net`;
}

function digestsMatch(expectedDigest: Buffer, provided: string): boolean {
    const providedDigest = createHash("sha256").update(provided, "utf8").digest();
    return timingSafeEqual(expectedDigest, providedDigest);
}
