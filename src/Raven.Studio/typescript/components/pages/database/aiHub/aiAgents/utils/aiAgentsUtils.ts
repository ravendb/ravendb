import moment from "moment";
import { AiAgentDocMessage, AiAgentMessage, AiAgentToolCall, AiAgentToolInfo, AiAgentToolType } from "./aiAgentsTypes";

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
    docMessages: AiAgentDocMessage[];
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
}

function mapMessagesFromDoc({ docMessages, config }: MapMessagesFromDocOptions): AiAgentMessage[] {
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

export const aiAgentsUtils = {
    messageDateFormat: "HH:mm A",
    getPrettifiedContent,
    mapMessagesFromDoc,
};
