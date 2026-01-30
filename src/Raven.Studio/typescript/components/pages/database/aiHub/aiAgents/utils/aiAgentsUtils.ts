import moment from "moment";
import { AiAgentDocMessage, AiAgentMessage } from "./aiAgentsTypes";

function getPrettifiedContent(content: string | Record<string, any>): string {
    if (content == null) {
        return null;
    }

    if (typeof content === "string") {
        try {
            return JSON.stringify(JSON.parse(content), null, 2);
        } catch {
            return content;
        }
    }

    return JSON.stringify(content, null, 2);
}

function getContentFromDoc(docMessage: AiAgentDocMessage): string {
    if (!docMessage) {
        return null;
    }

    return getPrettifiedContent(docMessage.content);
}

function mapMessageFromDoc(docMessage: AiAgentDocMessage): AiAgentMessage {
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
        toolCallId: docMessage.tool_call_id,
    };
}

function mergeToolResults(messages: AiAgentMessage[], allQueriesNames: string[]) {
    for (const message of messages) {
        if (message.toolCallId) {
            const messageWithToolCall = messages.find((x) => x.toolCalls.some((y) => y.id === message.toolCallId));
            const toolCall = messageWithToolCall.toolCalls.find((x) => x.id === message.toolCallId);
            const isQueryTool = allQueriesNames.some((name) => name === toolCall.name);

            if (toolCall && isQueryTool) {
                toolCall.queryToolResult = message;
            }

            message.toolName = toolCall.name;
        }
    }

    return messages;
}

export const aiAgentsUtils = {
    mapMessageFromDoc,
    messageDateFormat: "HH:mm A",
    mergeToolResults,
    getPrettifiedContent,
};
