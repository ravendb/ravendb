import aiAssistantAssistCommand from "commands/aiAssistant/assistAiAssistantCommand";
import checkConsentAiAssistantCommand from "commands/aiAssistant/checkConsentAiAssistantCommand";
import checkUsageAiAssistantCommand from "commands/aiAssistant/checkUsageAiAssistantCommand";
import giveConsentAiAssistantCommand from "commands/aiAssistant/giveConsentAiAssistantCommand";

export default class AiAssistantService {
    async assist(...args: ConstructorParameters<typeof aiAssistantAssistCommand>) {
        return new aiAssistantAssistCommand(...args).execute();
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
