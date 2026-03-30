import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class testAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: Raven.Server.Documents.Handlers.AI.Agents.AiAgentProcessorForTestConversation.AiAgentTestRequest,
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.AI.Agents.AiAgentProcessorForTestConversation.AiAgentTestResult> {
        const url = endpoints.databases.aiAgent.aiAgentTest;

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to test AI agent", response.responseText, response.statusText)
        );
    }
}

export = testAiAgentCommand;
