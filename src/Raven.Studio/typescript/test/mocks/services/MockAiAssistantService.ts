import { AssistAiAssistantResultDto } from "commands/aiAssistant/assistAiAssistantCommand";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";
import AiAssistantService from "components/services/AiAssistantService";
import { AutoMockService, MockedValue } from "test/mocks/services/AutoMockService";
import { AiAssistantStubs } from "test/stubs/AiAssistantStubs";

export default class MockAiAssistantService extends AutoMockService<AiAssistantService> {
    constructor() {
        super(new AiAssistantService());
    }

    withAssist(dto?: MockedValue<AssistAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.assist, dto, AiAssistantStubs.assistSuccess());
    }

    withCheckConsent(dto?: MockedValue<CheckConsentAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkConsent, dto, AiAssistantStubs.checkConsentSuccess());
    }

    withCheckUsage(dto?: MockedValue<CheckUsageAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkUsage, dto, AiAssistantStubs.checkUsageSuccess());
    }
}
