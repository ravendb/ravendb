export interface AiAgentToolCall {
    id: string;
    name: string;
    arguments: string;
}

export interface AiAgentMessage {
    id: string;
    role: "system" | "user" | "assistant" | "tool";
    content?: string;
    date?: string;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.Agents.AiUsage;
    toolCalls?: AiAgentToolCall[];
}
