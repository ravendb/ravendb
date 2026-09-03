import { describe, expect, it } from "vitest";
import {
    AI_CONSENT_REQUIRED_MESSAGE,
    createAssistantService,
    describeAssistantError,
    isAssistantConsentRequired,
    isQuillSessionExpired,
    type AssistantChatFrame,
    type AssistantStreamEvent,
} from "@/api/custom-services/assistant-service";
import { createApiClient, type ApiTransport } from "@/api/http-client";

function respondWithFrames(...frames: AssistantChatFrame[]) {
    const transport: ApiTransport = () =>
        Promise.resolve(
            new Response(frames.map((frame) => `data: ${JSON.stringify(frame)}\n\n`).join(""), {
                headers: { "Content-Type": "text/event-stream" },
            }),
        );

    return createApiClient({ transport });
}

async function streamEvents(client: ReturnType<typeof createApiClient>) {
    const events: AssistantStreamEvent[] = [];

    for await (const event of createAssistantService(client).stream({ message: "hi", conversationId: null })) {
        events.push(event);
    }

    return events;
}

function doneFrame(result: object): AssistantChatFrame {
    return { type: "Done", text: { ConversationId: "conversations/1", Status: "Success", ...result } };
}

describe("assistant stream", () => {
    it("accumulates the chunks and splices them into the answer the Done frame left empty", async () => {
        const client = respondWithFrames(
            { type: "Ongoing", text: "RavenDB " },
            { type: "Ongoing", text: "is a database." },
            doneFrame({ Response: { Answer: "", RelevantLinks: [] } }),
        );

        const events = await streamEvents(client);

        expect(events).toEqual([
            { type: "chunk", answer: "RavenDB " },
            { type: "chunk", answer: "RavenDB is a database." },
            {
                type: "done",
                result: {
                    ConversationId: "conversations/1",
                    Status: "Success",
                    Response: { Answer: "RavenDB is a database.", RelevantLinks: [] },
                },
            },
        ]);
    });

    it("keeps the answer the Done frame carried when nothing streamed", async () => {
        const client = respondWithFrames(doneFrame({ Response: { Answer: "Answered in one go." } }));

        const events = await streamEvents(client);

        expect(events).toEqual([
            {
                type: "done",
                result: {
                    ConversationId: "conversations/1",
                    Status: "Success",
                    Response: { Answer: "Answered in one go." },
                },
            },
        ]);
    });

    it("reports a Done frame that refused the turn as an error", async () => {
        const client = respondWithFrames(doneFrame({ Status: "OutOfTokens" }));

        await expect(streamEvents(client)).resolves.toEqual([
            {
                type: "error",
                message: "The AI assistant has used up its quota for now. Please try again later.",
                status: "OutOfTokens",
            },
        ]);
    });

    it("reports an Error frame", async () => {
        const client = respondWithFrames({ type: "Ongoing", text: "Half an ans" }, { type: "Error" });

        const events = await streamEvents(client);

        expect(events.at(-1)).toEqual({
            type: "error",
            message: "The AI assistant is unavailable right now. Please try again later.",
            status: "InternalError",
        });
    });

    it("reports a stream that ended without a Done frame", async () => {
        const client = respondWithFrames({ type: "Ongoing", text: "Half an ans" });

        const events = await streamEvents(client);

        expect(events.at(-1)).toEqual({
            type: "error",
            message: "The AI assistant did not finish answering. Please try again.",
        });
    });

    it("reports a stream cut off mid-frame as unfinished rather than malformed", async () => {
        const client = createApiClient({
            transport: () =>
                Promise.resolve(
                    new Response('data: {"type":"Ongoing","text":"Half"}\n\ndata: {"type":"Ongo', {
                        headers: { "Content-Type": "text/event-stream" },
                    }),
                ),
        });

        const events = await streamEvents(client);

        expect(events.at(-1)).toEqual({
            type: "error",
            message: "The AI assistant did not finish answering. Please try again.",
        });
    });
});

async function refusalOf(body: string, status: number, contentType: string) {
    const client = createApiClient({
        transport: () => Promise.resolve(new Response(body, { status, headers: { "Content-Type": contentType } })),
    });

    try {
        await streamEvents(client);
    } catch (error) {
        return error;
    }

    return null;
}

async function errorOf(body: string, status: number, contentType: string) {
    return describeAssistantError(await refusalOf(body, status, contentType));
}

describe("describeAssistantError", () => {
    it("reads the Status the AI service put in its relayed refusal", async () => {
        const message = await errorOf('{"Status":"ConsentRequired"}', 401, "application/json");

        expect(message).toBe(AI_CONSENT_REQUIRED_MESSAGE);
    });

    it("falls back to the status code for a refusal that carries no JSON body", async () => {
        const message = await errorOf("Request body too large", 413, "text/plain");

        expect(message).toBe("That message is too large for the AI assistant. Please shorten it and try again.");
    });

    it("keeps Quill's own message for a request it rejected itself", async () => {
        const message = await errorOf('{"error":"message is required"}', 400, "application/json");

        expect(message).toBe("message is required");
    });
});

describe("isAssistantConsentRequired", () => {
    it("recognizes the refusal that asks for consent", async () => {
        const error = await refusalOf('{"Status":"ConsentRequired"}', 401, "application/json");

        expect(isAssistantConsentRequired(error)).toBe(true);
    });

    it("does not read a rejected license as a missing consent", async () => {
        const error = await refusalOf("Unauthorized", 401, "text/plain");

        expect(isAssistantConsentRequired(error)).toBe(false);
    });
});

describe("isQuillSessionExpired", () => {
    it("recognizes Quill's bare 401 session challenge", async () => {
        const error = await refusalOf("", 401, "text/plain");

        expect(isQuillSessionExpired(error)).toBe(true);
    });

    it("does not read the AI service's relayed refusals as an expired session", async () => {
        const consentError = await refusalOf('{"Status":"ConsentRequired"}', 401, "application/json");
        const licenseError = await refusalOf("Unauthorized", 401, "text/plain");

        expect(isQuillSessionExpired(consentError)).toBe(false);
        expect(isQuillSessionExpired(licenseError)).toBe(false);
    });
});
