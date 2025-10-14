import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import aiAgentsTypes = require("components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes");

export interface RunAiAgentRequestDto extends Omit<Raven.Client.Documents.Operations.AI.Agents.ConversionRequestBody, "UserPrompt"> {
    UserPrompt: string | { type: "text", text: string }[];
}

export default class runAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: RunAiAgentRequestDto,
        private agentId: string,
        private conversationId: string,
        private changeVector: string
    ) {
        super();
    }

    execute(): JQueryPromise<aiAgentsTypes.AiAgentRunResult> {
        const args = {
            agentId: this.agentId,
            conversationId: this.conversationId,
            changeVector: this.changeVector
        };

        const url = endpoints.databases.aiAgent.aiAgent + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to run AI agent", response.responseText, response.statusText)
        );
    }
}
