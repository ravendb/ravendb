import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type RequestDto = Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration & {
    Parameters: Record<string, string>;
    Prompt: string;
};

class testAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: RequestDto
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.AI.Agents.ChatResult<object>> {
        const url = endpoints.databases.aiAgent.aiAgentTest;

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to test AI agent", response.responseText, response.statusText)
        );
    }
}

export = testAiAgentCommand;
