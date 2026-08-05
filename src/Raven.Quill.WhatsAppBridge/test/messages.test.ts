import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { classifyMessage } from "../src/messages.js";

const key = (overrides: object = {}) => ({
    remoteJid: "48123456789@s.whatsapp.net",
    fromMe: false,
    id: "MSG1",
    ...overrides,
});

describe("classifyMessage", () => {
    it("classifies a plain conversation text", () => {
        const result = classifyMessage({
            key: key(),
            message: { conversation: "hello" },
            messageTimestamp: 1754300000,
        });

        assert.deepEqual(result, {
            sender: "48123456789@s.whatsapp.net",
            messageId: "MSG1",
            kind: "text",
            text: "hello",
            timestamp: 1754300000,
        });
    });

    it("classifies extended text (replies, links)", () => {
        const result = classifyMessage({
            key: key(),
            message: { extendedTextMessage: { text: "quoted reply" } },
        });

        assert.equal(result?.kind, "text");
        assert.equal(result?.text, "quoted reply");
    });

    it("unwraps ephemeral messages", () => {
        const result = classifyMessage({
            key: key(),
            message: { ephemeralMessage: { message: { conversation: "disappearing" } } },
        });

        assert.equal(result?.text, "disappearing");
    });

    it("converts Long-style timestamps", () => {
        const result = classifyMessage({
            key: key(),
            message: { conversation: "hi" },
            messageTimestamp: { toNumber: () => 1754300001 },
        });

        assert.equal(result?.timestamp, 1754300001);
    });

    it("ignores own messages", () => {
        assert.equal(classifyMessage({ key: key({ fromMe: true }), message: { conversation: "me" } }), null);
    });

    it("ignores group, broadcast, newsletter and status chats", () => {
        for (const remoteJid of [
            "1234-5678@g.us",
            "123@broadcast",
            "status@broadcast",
            "123@newsletter",
        ]) {
            assert.equal(classifyMessage({ key: key({ remoteJid }), message: { conversation: "x" } }), null);
        }
    });

    it("ignores protocol-only and reaction payloads", () => {
        assert.equal(classifyMessage({ key: key(), message: { protocolMessage: {} } }), null);
        assert.equal(classifyMessage({ key: key(), message: { reactionMessage: {} } }), null);
        assert.equal(classifyMessage({ key: key(), message: { messageContextInfo: {} } }), null);
        assert.equal(classifyMessage({ key: key(), message: null }), null);
    });

    it("maps media to unsupported with no text", () => {
        for (const media of ["imageMessage", "audioMessage", "videoMessage", "stickerMessage", "documentMessage", "locationMessage"]) {
            const result = classifyMessage({ key: key(), message: { [media]: {} } });
            assert.equal(result?.kind, "unsupported", media);
            assert.equal(result?.text, null);
        }
    });

    it("treats whitespace-only text as unsupported", () => {
        const result = classifyMessage({ key: key(), message: { conversation: "   " } });
        assert.equal(result?.kind, "unsupported");
    });
});
