import moment from "moment";
import { AiAgentDocMessage, AiAgentMessage } from "./aiAgentsTypes";

function getContentFromDoc(docMessage: AiAgentDocMessage): string {
    if (docMessage.content && (docMessage.role === "assistant" || docMessage.role === "tool")) {
        return JSON.stringify(JSON.parse(docMessage.content), null, 2);
    }
    return docMessage.content;
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

export const aiAgentsUtils = {
    mapMessageFromDoc,
    messageDateFormat: "HH:mm A",
};
