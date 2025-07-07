import testAiAgentCommand from "commands/database/aiAgents/testAiAgentCommand";
import saveAiAgentCommand from "commands/database/aiAgents/saveAiAgentCommand";
import getAiAgentCommand from "commands/database/aiAgents/getAiAgentCommand";
import deleteAiAgentCommand from "commands/database/aiAgents/deleteAiAgentCommand";
import startAiAgentCommand from "commands/database/aiAgents/startAiAgentCommand";
import resumeAiAgentCommand from "commands/database/aiAgents/resumeAiAgentCommand";

export default class AiAgentService {
    async getAiAgents(...args: ConstructorParameters<typeof getAiAgentCommand>) {
        return new getAiAgentCommand(...args).execute();
    }

    async saveAiAgent(...args: ConstructorParameters<typeof saveAiAgentCommand>) {
        return new saveAiAgentCommand(...args).execute();
    }

    async deleteAiAgent(...args: ConstructorParameters<typeof deleteAiAgentCommand>) {
        return new deleteAiAgentCommand(...args).execute();
    }

    async startAiAgent(...args: ConstructorParameters<typeof startAiAgentCommand>) {
        return new startAiAgentCommand(...args).execute();
    }

    async resumeAiAgent(...args: ConstructorParameters<typeof resumeAiAgentCommand>) {
        return new resumeAiAgentCommand(...args).execute();
    }

    async testAiAgent(...args: ConstructorParameters<typeof testAiAgentCommand>) {
        return new testAiAgentCommand(...args).execute();
    }
}
