import testAiAgentCommand from "commands/database/aiAgents/testAiAgentCommand";
import saveAiAgentCommand from "commands/database/aiAgents/saveAiAgentCommand";
import getAiAgentCommand from "commands/database/aiAgents/getAiAgentCommand";
import deleteAiAgentCommand from "commands/database/aiAgents/deleteAiAgentCommand";
import runAiAgentCommand from "commands/database/aiAgents/runAiAgentCommand";
import generateCodeAiAgentCommand from "commands/database/aiAgents/generateCodeAiAgentCommand";

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

    async runAiAgent(...args: ConstructorParameters<typeof runAiAgentCommand>) {
        return new runAiAgentCommand(...args).execute();
    }

    async testAiAgent(...args: ConstructorParameters<typeof testAiAgentCommand>) {
        return new testAiAgentCommand(...args).execute();
    }

    async generateCode(...args: ConstructorParameters<typeof generateCodeAiAgentCommand>) {
        return new generateCodeAiAgentCommand(...args).execute();
    }
}
