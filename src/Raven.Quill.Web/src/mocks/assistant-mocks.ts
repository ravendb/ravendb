import { delay, http, HttpResponse } from "msw";
import type { AssistantChatFrame } from "@/api/custom-services/assistant-service";
import type { AssistantConsentResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

// assistant/chat relays the AI service's Server-Sent Events rather than serving a body the OpenAPI
// contract describes, so this mock uses plain msw instead of `apiHttp` — mirroring appsMocks.setupTry.
export const assistantMocks = {
    chat: (frames: AssistantChatFrame[] = sampleAssistantFrames, chunkDelayMs = 120) =>
        http.post("/api/assistant/chat", () => {
            const encoder = new TextEncoder();
            const stream = new ReadableStream<Uint8Array>({
                async start(controller) {
                    for (const frame of frames) {
                        await delay(chunkDelayMs);
                        controller.enqueue(encoder.encode(`data: ${JSON.stringify(frame)}\n\n`));
                    }

                    controller.close();
                },
            });

            return new HttpResponse(stream, { headers: { "Content-Type": "text/event-stream" } });
        }),
    consent: (response: AssistantConsentResponse = { status: "Success" }) =>
        apiHttp.get("/api/assistant/consent", ({ response: res }) => res(200).json(response)),
    consentUnavailable: () =>
        apiHttp.get("/api/assistant/consent", ({ response: res }) =>
            res(502).json({ error: "The AI service could not be reached." }),
        ),
    giveConsent: (response: AssistantConsentResponse = { status: "Success" }) =>
        apiHttp.post("/api/assistant/consent", ({ response: res }) => res(200).json(response)),
};

const sampleAssistantFrames: AssistantChatFrame[] = [
    { type: "Ongoing", text: "RavenDB indexes are " },
    { type: "Ongoing", text: "**precomputed** query results, kept up to date in the background.\n\n" },
    { type: "Ongoing", text: "Create one from the Studio, or let the server pick an auto index for you." },
    {
        type: "Done",
        // The service sends an empty Answer: the answer is the chunks above.
        text: {
            ConversationId: "conversations/1",
            Status: "Success",
            UsagePercentage: 1.5,
            Response: {
                Answer: "",
                RelevantLinks: [
                    {
                        Title: "Indexes overview",
                        Url: "https://ravendb.net/docs/article-page/latest/csharp/indexes/what-are-indexes",
                    },
                ],
                FollowUpQuestions: ["How do I create a static index?"],
            },
        },
    },
];
