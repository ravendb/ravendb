import AiAgentService from "components/services/AiAgentService";
import { AutoMockService } from "test/mocks/services/AutoMockService";

export default class MockAiAgentService extends AutoMockService<AiAgentService> {
    constructor() {
        super(new AiAgentService());
    }
}
