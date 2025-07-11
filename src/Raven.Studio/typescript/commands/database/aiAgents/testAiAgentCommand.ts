import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

interface Document {
    Agent: string;
    Parameters: TODO;
    Messages: TODO[];
    TotalUsage: Raven.Client.Documents.Operations.AI.Agents.AiUsage;
    OpenActionCalls: TODO;
}


interface TestAiAgentResult {
    Document: Document;
    Response: TODO;
    ToolRequests: Raven.Client.Documents.Operations.AI.Agents.AiAgentActionRequest[];
    Usage: Raven.Client.Documents.Operations.AI.Agents.AiUsage;
}

class testAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: Raven.Server.Documents.Handlers.AI.Agents.AiAgentProcessorForTestConversation.AiAgentTestRequest,
    ) {
        super();
    }

    execute(): JQueryPromise<TestAiAgentResult> {
        const url = endpoints.databases.aiAgent.aiAgentTest;

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to test AI agent", response.responseText, response.statusText)
        );
    }
}

export = testAiAgentCommand;
