import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import buildInfo = require("models/resources/buildInfo");

interface RunChatbotAssistAiAssistantRequestDto {
    OperationType: "Chatbot";
    View: string;
    Message: string;
    RavenVersion: string;
    ConversationId?: string;
    ActionsResponses?: Record<string, any>;
    AdditionalAttachedContext?: Record<string, any>;
}

export type RunChatbotAiAssistantViewData = Omit<
    RunChatbotAssistAiAssistantRequestDto,
    "OperationType" | "RavenVersion"
>;

export interface ChatbotRelevantLink {
    Title: string;
    Url: string;
}

export type AdditionalContextOption = "DatabaseName" | "IndexName" | "CollectionName" | "DocumentId";

export interface RunChatbotAiAssistantResultDto {
    ConversationId: string;
    Status: AiAssistantResponseStatus;
    UsagePercentage: number;
    Response: {
        Answer: string;
        RelevantLinks: ChatbotRelevantLink[];
        FollowUpQuestions: string[];
    };
    AdditionalContext: Record<
        string,
        {
            Message: string;
            Option: AdditionalContextOption;
        }
    >;
    Endpoints: Record<string, string[]>; // <toolId, endpointUrl[]>
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
