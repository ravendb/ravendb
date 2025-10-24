import refinePromptAiAssistantCommand from "commands/aiAssistant/refinePromptAiAssistantCommand";
import checkConsentAiAssistantCommand from "commands/aiAssistant/checkConsentAiAssistantCommand";
import checkUsageAiAssistantCommand from "commands/aiAssistant/checkUsageAiAssistantCommand";
import giveConsentAiAssistantCommand from "commands/aiAssistant/giveConsentAiAssistantCommand";
import runChatbotAiAssistantCommand from "commands/aiAssistant/runChatbotAiAssistantCommand";

export default class AiAssistantService {
    async refinePrompt(...args: ConstructorParameters<typeof refinePromptAiAssistantCommand>) {
        return new refinePromptAiAssistantCommand(...args).execute();
    }

    async runChatbot(...args: ConstructorParameters<typeof runChatbotAiAssistantCommand>) {
        return new runChatbotAiAssistantCommand(...args).execute();
    }

    async checkConsent(...args: ConstructorParameters<typeof checkConsentAiAssistantCommand>) {
        return new checkConsentAiAssistantCommand(...args).execute();
    }

    async giveConsent(...args: ConstructorParameters<typeof giveConsentAiAssistantCommand>) {
        return new giveConsentAiAssistantCommand(...args).execute();
    }

    async checkUsage(...args: ConstructorParameters<typeof checkUsageAiAssistantCommand>) {
        return new checkUsageAiAssistantCommand(...args).execute();
    }
}
