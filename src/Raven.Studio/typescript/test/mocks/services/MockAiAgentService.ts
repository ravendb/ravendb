import { AiAgentGenerateCodeResultDto } from "commands/database/aiAgents/generateCodeAiAgentCommand";
import AiAgentService from "components/services/AiAgentService";
import { AutoMockService, MockedValue } from "test/mocks/services/AutoMockService";
import { AiAgentStubs } from "test/stubs/AiAgentStubs";

export default class MockAiAgentService extends AutoMockService<AiAgentService> {
    constructor() {
        super(new AiAgentService());
    }

    withAiAgents(dto?: MockedValue<GetAiAgentResultDto>) {
        return this.mockResolvedValue(this.mocks.getAiAgents, dto, AiAgentStubs.getAiAgents());
    }

    withGenerateCode(dto?: MockedValue<AiAgentGenerateCodeResultDto>) {
        return this.mockResolvedValue(this.mocks.generateCode, dto, AiAgentStubs.getGeneratedCode());
    }
}
