import { RefinePromptAiAssistantResultDto } from "commands/aiAssistant/refinePromptAiAssistantCommand";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";

export class AiAssistantStubs {
    static refinePromptSuccess(): RefinePromptAiAssistantResultDto {
        return {
            UsagePercentage: 1,
            RefinedPrompt: "This is your refined prompt",
            Status: "Success",
        };
    }

    static checkConsentSuccess(): CheckConsentAiAssistantResultDto {
        return { Status: "Success" };
    }

    static checkUsageSuccess(): CheckUsageAiAssistantResultDto {
        return { Status: "Success", UsagePercentage: 35 };
    }
}
