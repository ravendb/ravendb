import moment from "moment";
import { AiAgentDocMessage, AiAgentDocumentResponse, AiAgentMessage, AiAgentRunResult } from "./aiAgentsTypes";

const getContentFromDoc = (docMessage: AiAgentDocMessage): string => {
    if (docMessage.content && (docMessage.role === "assistant" || docMessage.role === "tool")) {
        return JSON.stringify(JSON.parse(docMessage.content), null, 2);
    }
    return docMessage.content;
};

const mapMessageFromDoc = (docMessage: AiAgentDocMessage): AiAgentMessage => {
    return {
        id: _.uniqueId(),
        role: docMessage.role,
        content: getContentFromDoc(docMessage),
        state: "success",
        toolCalls: docMessage.tool_calls
            ? docMessage.tool_calls.map((x) => ({
                  id: x.id,
                  name: x.function.name,
                  arguments: x.function.arguments,
              }))
            : [],
        date: docMessage.date ? moment(docMessage.date).format(aiAgentsUtils.messageDateFormat) : null,
        usage: docMessage.usage,
    };
};

const getContentFromResponse = (dto: AiAgentRunResult): string => {
    if (dto.Response) {
        return JSON.stringify(dto.Response, null, 2);
    }
    return dto.Response;
};

const mapMessageFromResponse = (
    dto: AiAgentRunResult,
    id: string,
    document?: AiAgentDocumentResponse
): AiAgentMessage => {
    return {
        id,
        role: "assistant",
        content: getContentFromResponse(dto),
        state: "success",
        date: "TODO date",
        toolCalls:
            dto.ActionRequests?.map((x) => ({
                id: x.ToolId,
                name: x.Name,
                arguments: x.Arguments,
            })) ?? [],
        transcript: document?.Messages.map((x: AiAgentDocMessage) => mapMessageFromDoc(x)) ?? [],
        usage: dto.Usage,
    };
};

export const aiAgentsUtils = {
    mapMessageFromDoc,
    mapMessageFromResponse,
    messageDateFormat: "HH:mm A",
};
