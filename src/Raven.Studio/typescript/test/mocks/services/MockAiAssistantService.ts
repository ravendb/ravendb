import { AssistAiAssistantResultDto } from "commands/aiAssistant/assistAiAssistantCommand";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";
import AiAssistantService from "components/services/AiAssistantService";
import { AutoMockService, MockedValue } from "test/mocks/services/AutoMockService";

export default class MockAiAssistantService extends AutoMockService<AiAssistantService> {
    constructor() {
        super(new AiAssistantService());
    }

    withAssist(dto?: MockedValue<AssistAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.assist, dto, {
            InputTokenCount: 10,
            OutputTokenCount: 20,
            UsagePercentage: 1,
            RefinedPrompt: "This is your refined prompt",
            RefinedText: "This is your refined text",
            Status: "Success",
        });
    }

    withCheckConsent(dto?: MockedValue<CheckConsentAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkConsent, dto, { Status: "Success" });
    }

    withCheckUsage(dto?: MockedValue<CheckUsageAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkUsage, dto, { Status: "Success", UsagePercentage: 35 });
    }
}
