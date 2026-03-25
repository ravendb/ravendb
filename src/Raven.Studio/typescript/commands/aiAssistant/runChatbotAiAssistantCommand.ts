import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export interface RunChatbotAssistAiAssistantRequestDto {
    OperationType: "Chatbot";
    Message: string;
    RavenVersion: number;
    ConversationId?: string;
    ActionsResponses?: Record<string, any>;
    AdditionalAttachedContext?: Record<string, any>;
}

export type RunChatbotAiAssistantViewData = Omit<
    RunChatbotAssistAiAssistantRequestDto,
    "OperationType"
>;

export function getRunChatbotAssistAiAssistantRequestDto(
    viewData: RunChatbotAiAssistantViewData
): RunChatbotAssistAiAssistantRequestDto {
    return {
        OperationType: "Chatbot",
        ...viewData,
    };
}

export interface ChatbotRelevantLink {
    Title: string;
    Url: string;
}

export interface RunChatbotAiAssistantResultDto {
    ConversationId: string;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    Response: {
        Answer: string;
        RelevantLinks: ChatbotRelevantLink[];
        FollowUpQuestions: string[];
    };
    Endpoints: Record<string, string[]>;
}

export default class runChatbotAiAssistantCommand extends commandBase {
    constructor(private viewData: RunChatbotAiAssistantViewData, private abortSignal?: AbortSignal) {
        super();
    }

    execute() {
        const relativeUrl = endpoints.global.aiAssistant.assistantAssist;
        const dto = getRunChatbotAssistAiAssistantRequestDto(this.viewData);

        return this.fetch({
            relativeUrl,
            options: {
                method: "POST",
                body: JSON.stringify(dto),
                signal: this.abortSignal,
            },
        });
    }
}
