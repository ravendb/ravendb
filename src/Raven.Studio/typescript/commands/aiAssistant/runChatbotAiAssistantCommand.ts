import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import buildInfo = require("models/resources/buildInfo");

interface RunChatbotAssistAiAssistantRequestDto {
    OperationType: "Chatbot";
    View: string;
    Message: string;
    RavenVersion: string;
    ConversationId?: string;
}

export type RunChatbotAiAssistantViewData = Omit<RunChatbotAssistAiAssistantRequestDto, "OperationType" | "RavenVersion">;

export interface ChatbotRelevantLink {
    Title: string;
    Url: string;
}

export interface RunChatbotAiAssistantResultDto {
    ConversationId: string;
    InputTokenCount: number;
    OutputTokenCount: number;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    Response: {
        Answer: string;
        RelevantLinks: ChatbotRelevantLink[];
        FollowUpQuestions: string[];
    };
}

export default class runChatbotAiAssistantCommand extends commandBase {
    constructor(private viewData: RunChatbotAiAssistantViewData) {
        super();
    }

    execute() {
        const relativeUrl = endpoints.global.aiAssistant.assistantAssist + "?streaming=true";

        const dto: RunChatbotAssistAiAssistantRequestDto = {
            OperationType: "Chatbot",
            RavenVersion: buildInfo.serverBuildVersion()?.ProductVersion ?? "latest",
            ...this.viewData,
        };

        return this.fetch({
            relativeUrl,
            options: {
                method: "POST",
                body: JSON.stringify(dto),
            },
        });
    }
}
