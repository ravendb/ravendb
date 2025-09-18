import { AssistAiAssistantResultDto } from "commands/aiAssistant/assistAiAssistantCommand";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import AiAssistantService from "components/services/AiAssistantService";
import { AutoMockService, MockedValue } from "test/mocks/services/AutoMockService";
import { AiAgentStubs } from "test/stubs/AiAgentStubs";
import { AiAssistantStubs } from "test/stubs/AiAssistantStubs";

export default class MockAiAssistantService extends AutoMockService<AiAssistantService> {
    constructor() {
        super(new AiAssistantService());
    }

    withAssist(dto?: MockedValue<AssistAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.assist, dto, AiAssistantStubs.assist());
    }

    withCheckConsent(dto?: MockedValue<CheckConsentAiAssistantResultDto>) {
        return this.mockResolvedValue(this.mocks.checkConsent, dto, { Status: "Success" });
    }
}
