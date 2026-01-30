import { RefinePromptAiAssistantResultDto } from "commands/aiAssistant/refinePromptAiAssistantCommand";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";
import AiAssistantService from "components/services/AiAssistantService";
import { AutoMockService, MockedValue } from "test/mocks/services/AutoMockService";
import { AiAssistantStubs } from "test/stubs/AiAssistantStubs";

export default class MockAiAssistantService extends AutoMockService<AiAssistantService> {
    constructor() {
        super(new AiAssistantService());
    }

    withRefinePrompt(dto?: MockedValue<RefinePromptAiAssistantResultDto>) {
        return this.mockFetchResponse(this.mocks.refinePrompt, dto, AiAssistantStubs.refinePromptSuccess());
    }

    withCheckConsent(dto?: MockedValue<CheckConsentAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkConsent, dto, AiAssistantStubs.checkConsentSuccess());
    }

    withCheckUsage(dto?: MockedValue<CheckUsageAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkUsage, dto, AiAssistantStubs.checkUsageSuccess());
    }
}
