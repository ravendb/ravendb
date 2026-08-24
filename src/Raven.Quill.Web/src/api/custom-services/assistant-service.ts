import { API_ENDPOINTS, type AssistantChatRequest } from "@/api/generated/server-api";
import { streamSseData } from "@/api/custom-services/response-stream";
import { isApiError, type ApiClient } from "@/api/http-client";

/** How the AI service reports the outcome of a turn, in its own words. */
export type AssistantChatStatus =
    | "Success"
    | "InvalidCredentials"
    | "InvalidData"
    | "ConsentRequired"
    | "OutOfTokens"
    | "RequestTooLarge"
    | "Aborted"
    | "InternalError";

export type AssistantRelevantLink = {
    Title: string;
    Url: string;
};

/** The AI service's chatbot result. The backend relays the assist stream untouched — the same way
 * RavenDB's own /assistant/assist proxy does — so this is the service's shape and casing rather
 * than Quill's, and it stays in step with the DTO the RavenDB Studio consumes. */
export type AssistantChatResult = {
    ConversationId?: string | null;
    Status?: AssistantChatStatus;
    UsagePercentage?: number;
    Response?: {
        Answer: string;
        RelevantLinks?: AssistantRelevantLink[];
        FollowUpQuestions?: string[];
    };
    Endpoints?: Record<string, string[]>;
};

/** One Server-Sent Events frame: an `Ongoing` answer chunk, the terminal `Done` result, or `Error`. */
export type AssistantChatFrame = {
    type?: "Ongoing" | "Done" | "Error";
    text?: string | AssistantChatResult | null;
};

export type AssistantStreamEvent =
    | { type: "chunk"; answer: string }
    | { type: "done"; result: AssistantChatResult }
    /** `status` carries the AI service's own word for the refusal when it named one. */
    | { type: "error"; message: string; status?: AssistantChatStatus };

export function createAssistantService(client: ApiClient) {
    return {
        stream: async function* (
            request: AssistantChatRequest,
            signal?: AbortSignal,
        ): AsyncGenerator<AssistantStreamEvent> {
            let answer = "";

            for await (const payload of streamSseData(client, API_ENDPOINTS.assistant.chat, request, signal)) {
                const frame = parseFrame(payload);

                if (frame.type === "Ongoing" && typeof frame.text === "string") {
                    answer += frame.text;
                    yield { type: "chunk", answer };
                    continue;
                }

                if (frame.type === "Done" && typeof frame.text === "object" && frame.text !== null) {
                    const result = spliceAnswer(frame.text, answer);
                    yield result.Status === "Success" || result.Status === undefined
                        ? { type: "done", result }
                        : { type: "error", message: describeAssistantStatus(result.Status), status: result.Status };
                    return;
                }

                if (frame.type === "Error") {
                    yield {
                        type: "error",
                        message: describeAssistantStatus("InternalError"),
                        status: "InternalError",
                    };
                    return;
                }
            }

            yield { type: "error", message: "The AI assistant did not finish answering. Please try again." };
        },
    };
}

export type AssistantService = ReturnType<typeof createAssistantService>;

/** Turns a failed request into an operator-facing sentence. The AI service reports its refusals as a
 * `Status` in the error body (401 ConsentRequired, 429 OutOfTokens) and the backend relays those
 * untouched, so they are read from there first, then from the status code for the refusals that carry
 * no JSON body; anything else keeps the request's own message. */
export function describeAssistantError(error: unknown) {
    if (isApiError(error)) {
        const status = readStatus(error.details) ?? ASSISTANT_STATUS_BY_HTTP_CODE[error.status];

        if (status) {
            return describeAssistantStatus(status);
        }
    }

    return error instanceof Error ? error.message : describeAssistantStatus("InternalError");
}

export const AI_CONSENT_REQUIRED_MESSAGE =
    "The RavenDB AI service needs your consent before it can answer. Open the AI assistant panel to review and accept the Terms of Use.";

/** True when a request was refused for want of that consent rather than for any other reason. The AI
 * service says so in the `Status` of its 401 body, which the backend relays untouched. */
export function isAssistantConsentRequired(error: unknown) {
    return isApiError(error) && readStatus(error.details) === "ConsentRequired";
}

/** True for Quill's own authentication challenge: an expired operator session answers with a bare
 * 401 and no body at all, while the AI service's relayed refusals carry a body. */
export function isQuillSessionExpired(error: unknown) {
    return isApiError(error) && error.status === 401 && error.details === undefined;
}

export const AI_LICENSE_UNAVAILABLE_MESSAGE =
    "The AI assistant is not available for this appliance's license. Please contact support.";

const ASSISTANT_STATUS_MESSAGES: Partial<Record<AssistantChatStatus, string>> = {
    ConsentRequired: AI_CONSENT_REQUIRED_MESSAGE,
    InvalidCredentials: AI_LICENSE_UNAVAILABLE_MESSAGE,
    InvalidData: "The AI assistant could not make sense of that request.",
    OutOfTokens: "The AI assistant has used up its quota for now. Please try again later.",
    RequestTooLarge: "That message is too large for the AI assistant. Please shorten it and try again.",
};

// A refusal the service answers in plain text rather than JSON — a 413 is the one that reaches an
// operator in practice — leaves nothing for readStatus to find, so the code stands in for the Status.
const ASSISTANT_STATUS_BY_HTTP_CODE: Partial<Record<number, AssistantChatStatus>> = {
    401: "InvalidCredentials",
    413: "RequestTooLarge",
    429: "OutOfTokens",
};

function describeAssistantStatus(status: AssistantChatStatus) {
    return ASSISTANT_STATUS_MESSAGES[status] ?? "The AI assistant is unavailable right now. Please try again later.";
}

// The service leaves Response.Answer empty — the answer exists only as the Ongoing chunks — so it is
// spliced back in before anything reads the result, the way the Studio does. A turn that streamed no
// chunks at all keeps whatever the Done frame carried rather than being blanked out.
function spliceAnswer(result: AssistantChatResult, answer: string): AssistantChatResult {
    return { ...result, Response: { ...result.Response, Answer: answer || (result.Response?.Answer ?? "") } };
}

function parseFrame(payload: string): AssistantChatFrame {
    try {
        return JSON.parse(payload) as AssistantChatFrame;
    } catch {
        throw new Error("The AI assistant sent a malformed response.");
    }
}

function readStatus(details: unknown): AssistantChatStatus | undefined {
    if (typeof details === "object" && details !== null && "Status" in details) {
        const status = (details as { Status: unknown }).Status;
        return typeof status === "string" ? (status as AssistantChatStatus) : undefined;
    }

    return undefined;
}
