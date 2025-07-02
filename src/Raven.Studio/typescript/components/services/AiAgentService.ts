import testAiAgentCommand from "commands/database/aiAgents/testAiAgentCommand";
import saveAiAgentCommand from "commands/database/aiAgents/saveAiAgentCommand";
import getAiAgentCommand from "commands/database/aiAgents/getAiAgentCommand";

export default class AiAgentService {
    async getAiAgents(...args: ConstructorParameters<typeof getAiAgentCommand>) {
        return new getAiAgentCommand(...args).execute();
    }

    async saveAiAgent(...args: ConstructorParameters<typeof saveAiAgentCommand>) {
        return new saveAiAgentCommand(...args).execute();
    }

    async testAiAgent(...args: ConstructorParameters<typeof testAiAgentCommand>) {
        return new testAiAgentCommand(...args).execute();
    }
}
