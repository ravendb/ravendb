export interface AiAgentToolCall {
    id: string;
    name: string;
    arguments: string;
    queryToolResult?: AiAgentMessage;
}

export type AiAgentRole = "system" | "user" | "assistant" | "tool";

export interface AiAgentMessage {
    id: string;
    role: AiAgentRole;
    content?: string;
    date?: string;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.AiUsage;
    toolCalls?: AiAgentToolCall[];
    toolCallId?: string;
    toolName?: string;
}

export interface AiAgentDocumentResponse {
    Agent: string;
    Parameters: TODO;
    Messages: TODO[];
    TotalUsage: Raven.Client.Documents.Operations.AI.AiUsage;
    OpenActionCalls: TODO;
}

export interface AiAgentRunResult {
    ConversationId?: string;
    Document?: AiAgentDocumentResponse;
    Response: any;
    ActionRequests: Raven.Client.Documents.Operations.AI.Agents.AiAgentActionRequest[];
    Usage: Raven.Client.Documents.Operations.AI.AiUsage;
}

export interface AiAgentDocMessage {
    role: AiAgentRole;
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
}
