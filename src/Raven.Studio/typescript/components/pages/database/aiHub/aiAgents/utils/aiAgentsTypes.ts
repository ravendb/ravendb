interface AiAgentToolCallBase {
    id: string;
    name: string;
    arguments: string;
    responseMessage?: AiAgentMessage;
}

export interface AiAgentToolCallQuery extends AiAgentToolCallBase {
    type: "query";
    configDetails: Raven.Client.Documents.Operations.AI.Agents.AiAgentToolQuery;
}

export interface AiAgentToolCallAction extends AiAgentToolCallBase {
    type: "action";
    configDetails: Raven.Client.Documents.Operations.AI.Agents.AiAgentToolAction;
}

export interface AiAgentToolCallSubAgent extends AiAgentToolCallBase {
    type: "sub-agent";
    configDetails: Raven.Client.Documents.Operations.AI.Agents.AiAgentToolSubAgent;
}

// When someone removes a tool from the agent configuration but there are messages in the conversation related to that tool, we want to show it on the UI
export interface AiAgentToolCallUnknown extends AiAgentToolCallBase {
    type: "unknown";
    configDetails: null;
}

export type AiAgentToolCall =
    | AiAgentToolCallAction
    | AiAgentToolCallQuery
    | AiAgentToolCallSubAgent
    | AiAgentToolCallUnknown;

export type AiAgentToolType = AiAgentToolCall["type"];
export type AiAgentToolCallForType<TType extends AiAgentToolType> = Extract<AiAgentToolCall, { type: TType }>;
export type AiAgentToolInfoForType<TType extends AiAgentToolType> = Pick<
    AiAgentToolCallForType<TType>,
    "type" | "configDetails"
>;
export type AiAgentToolInfo = { [TType in AiAgentToolType]: AiAgentToolInfoForType<TType> }[AiAgentToolType];
export type AiAgentDocRole = "system" | "user" | "assistant" | "tool";
export type AiAgentMessageRole = AiAgentDocRole | "submitted-action-tool" | "assistant-summary";

export interface AiAgentMessageAttachment {
    name: string;
    contentType?: string;
}

export interface AiAgentMessage {
    id: string;
    role: AiAgentMessageRole;
    content?: string;
    date?: string;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.AiUsage;
    toolCalls?: AiAgentToolCall[];
    toolCallId?: string;
    toolName?: string;
    subConversationId?: string;
    attachments?: AiAgentMessageAttachment[];
}

export type AiAgentOpenActionCalls = Record<string, Raven.Client.Documents.Operations.AI.Agents.AiAgentActionRequest>;

export interface AiAgentDocumentResponse {
    Agent: string;
    Parameters: Record<string, any>;
    Messages: AiAgentDocMessage[];
    TotalUsage: Raven.Client.Documents.Operations.AI.AiUsage;
    OpenActionCalls: AiAgentOpenActionCalls;
}

export interface AiAgentRunResult {
    ConversationId?: string;
    Document?: AiAgentDocumentResponse;
    Response: any;
    ActionRequests: Raven.Client.Documents.Operations.AI.Agents.AiAgentActionRequest[];
    Usage: Raven.Client.Documents.Operations.AI.AiUsage;
}

export interface AiAgentDocMessage {
    role: AiAgentDocRole;
    content: string;
    tool_calls?: {
        id: string;
        type: string;
        function: {
            name: string;
            arguments: string;
        };
    }[];
    tool_call_id?: string;
    date?: string;
    usage?: Raven.Client.Documents.Operations.AI.AiUsage;
    subConversationId?: string;
}
