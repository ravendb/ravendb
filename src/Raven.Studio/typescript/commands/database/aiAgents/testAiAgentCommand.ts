import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class testAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private configuration: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration,
        private prompt: string,
        private parameters: Record<string, string>
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.AI.Agents.ChatResult<object>> {
        const url = endpoints.databases.aiAgent.aiAgentTest;

        const dto = {
            Configuration: this.configuration,
            Prompt: this.prompt,
            Parameters: this.parameters,
        };

        return this.post(url, JSON.stringify(dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to test AI agent", response.responseText, response.statusText)
        );
    }
}

export = testAiAgentCommand;
