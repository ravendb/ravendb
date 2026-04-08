import moment from "moment";
import {
    AiAgentDocMessage,
    AiAgentDocumentResponse,
    AiAgentMessage,
    AiAgentToolCall,
    AiAgentToolInfo,
    AiAgentToolType,
    AiAgentMessageAttachment,
} from "./aiAgentsTypes";

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

interface MapMessagesFromDocOptions {
    conversationDocument: documentDto;
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
}

function mapMessagesFromDoc({ conversationDocument, config }: MapMessagesFromDocOptions): AiAgentMessage[] {
    const docMessages = conversationDocument?.Messages as AiAgentDocMessage[];
    const docAttachments = conversationDocument?.["@metadata"]?.["@attachments"];

    if (!docMessages?.length || !config) {
        return [];
    }

    const summaryPrefix = config.ChatTrimming?.Tokens?.ResultPrefix ?? "Summary of previous conversation:";

    const formatDate = (date: string) => (date ? moment(date).format(aiAgentsUtils.messageDateFormat) : null);

    function getToolInfoByName(toolName: string): AiAgentToolInfo {
        const queryConfig = config.Queries?.find((tool) => tool.Name === toolName);
        if (queryConfig) {
            return { type: "query", configDetails: queryConfig };
        }

        const actionConfig = config.Actions?.find((tool) => tool.Name === toolName);
        if (actionConfig) {
            return { type: "action", configDetails: actionConfig };
        }

        const subAgentConfig = config.SubAgents?.find((tool) => tool.Identifier === toolName);
        if (subAgentConfig) {
            return { type: "sub-agent", configDetails: subAgentConfig };
        }

        return { type: "unknown", configDetails: null };
    }

    const getToolResponseMessageById = (id: string): AiAgentMessage => {
        const message = docMessages.find((message) => message.tool_call_id === id);
        if (!message) {
            return null;
        }

        return {
            id,
            role: message.role,
            content: getContentFromDoc(message),
            state: "success",
            subConversationId: message.subConversationId,
        };
    };

    const getMessageToolCalls = (docToolCalls: AiAgentDocMessage["tool_calls"]): AiAgentToolCall[] => {
        if (!docToolCalls) {
            return null;
        }

        return docToolCalls.map((x) => {
            const info = getToolInfoByName(x.function.name);
            const responseMessage = getToolResponseMessageById(x.id);

            return {
                id: x.id,
                name: x.function.name,
                arguments: x.function.arguments,
                responseMessage,
                ...info,
            } satisfies AiAgentToolCall;
        });
    };

    const getDocMessageAttachments = (docMessage: AiAgentDocMessage): AiAgentMessageAttachment[] => {
        if (typeof docMessage.content === "string" && docMessage.content.startsWith("[Attachments: ")) {
            return docMessage.content
                .substring("[Attachments: ".length, docMessage.content.length - 1)
                .split(", ")
                .map((name) => {
                    const trimmedName = name.trim();
                    const docAttachment = docAttachments?.find((a) => a.Name === trimmedName);
                    return { name: trimmedName, contentType: docAttachment?.ContentType };
                });
        }

        return null;
    };

    const toolCallsInfoById = new Map<string, { name: string; type: AiAgentToolType }>();
    docMessages.forEach((docMessage) => {
        docMessage.tool_calls?.forEach((toolCall) => {
            const { type } = getToolInfoByName(toolCall.function.name);
            toolCallsInfoById.set(toolCall.id, { name: toolCall.function.name, type });
        });
    });

    const messages: AiAgentMessage[] = [];
    for (const docMessage of docMessages) {
        // User parameters are filled by the server. We don't want to show it on the UI
        if (
            docMessage.role === "user" &&
            typeof docMessage.content === "string" &&
            docMessage.content?.startsWith("AI Agent Parameters")
        ) {
            continue;
        }

        if (
            docMessage.role === "assistant" &&
            typeof docMessage.content === "string" &&
            docMessage.content?.startsWith(summaryPrefix)
        ) {
            messages.push({
                id: docMessage.date,
                role: "assistant-summary",
                content: getContentFromDoc(docMessage),
                state: "success",
                date: formatDate(docMessage.date),
                usage: docMessage.usage,
            });

            continue;
        }

        // Message with "tool" role contains the tool call result.
        // If it's an action we show it as a separate message. For other ones it's nested inside the assistant message.
        if (docMessage.role === "tool") {
            const toolInfo = toolCallsInfoById.get(docMessage.tool_call_id);

            if (toolInfo?.type === "action") {
                messages.push({
                    id: docMessage.date,
                    role: "submitted-action-tool",
                    content: getContentFromDoc(docMessage),
                    state: "success",
                    date: formatDate(docMessage.date),
                    toolName: toolInfo.name,
                });
            }

            continue;
        }

        const attachments = getDocMessageAttachments(docMessage);
        if (attachments) {
            const lastAddedMessage = messages[messages.length - 1];

            // Attachments are treated as a separate 'user' message so we want to merge it with the previous 'user' message
            if (lastAddedMessage && lastAddedMessage.role === "user") {
                lastAddedMessage.attachments = attachments;
                continue;
            }

            // Attachments can be sent without any text prompt, in that case we want to show it as a separate message with empty content
            messages.push({
                id: docMessage.date,
                role: docMessage.role,
                content: null,
                state: "success",
                date: formatDate(docMessage.date),
                attachments,
            });
            continue;
        }

        messages.push({
            id: docMessage.date,
            role: docMessage.role,
            content: getContentFromDoc(docMessage),
            state: "success",
            toolCalls: getMessageToolCalls(docMessage.tool_calls),
            date: formatDate(docMessage.date),
            usage: docMessage.usage,
            toolCallId: docMessage.tool_call_id,
            subConversationId: docMessage.subConversationId,
        });
    }

    return messages;
}

function hasOpenActionCalls(conversationDocument: Partial<Pick<AiAgentDocumentResponse, "OpenActionCalls">>): boolean {
    return Object.keys(conversationDocument?.OpenActionCalls ?? {}).length > 0;
}

export const aiAgentsUtils = {
    messageDateFormat: "HH:mm A",
    getPrettifiedContent,
    mapMessagesFromDoc,
    hasOpenActionCalls,
};
