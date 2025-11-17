import moment from "moment";
import { AiAgentDocMessage, AiAgentMessage } from "./aiAgentsTypes";

function getPrettifiedContent(content: string): string {
    if (content == null) {
        return null;
    }

    try {
        return JSON.stringify(JSON.parse(content), null, 2);
    } catch {
        return content;
    }
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

function getAceEditorHeight(content: string, maxHeightInPx = 320): `${number}px` {
    if (!content) {
        return "100px";
    }

    const lineHeight = 26;
    const minimumLineCount = 4;
    const lineCount = content.split("\n").length;
    const effectiveLineCount = Math.max(lineCount, minimumLineCount);

    if (effectiveLineCount <= 12) {
        const halfLineHeight = lineHeight / 2; // to show that there is more content
        return `${effectiveLineCount * lineHeight + halfLineHeight}px`;
    }

    return `${maxHeightInPx}px`;
}

function getAceEditorMode(content: string): "json" | "text" {
    if (content?.startsWith("{") && content?.endsWith("}")) {
        return "json";
    }

    return "text";
}

export const aiAgentsUtils = {
    mapMessageFromDoc,
    messageDateFormat: "HH:mm A",
    mergeToolResults,
    getPrettifiedContent,
    getAceEditorHeight,
    getAceEditorMode,
};
