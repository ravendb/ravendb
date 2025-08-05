import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import aiAgentsTypes = require("components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes");

class runAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: Raven.Client.Documents.Operations.AI.Agents.ConversionRequestBody,
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

export = runAiAgentCommand;
