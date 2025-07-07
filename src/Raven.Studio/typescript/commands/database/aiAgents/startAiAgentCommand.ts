import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class startAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private name: string,
        private dto: Raven.Client.Documents.Operations.AI.Agents.StartChatBody
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.AI.Agents.ChatResult<object>> {
        const args = {
            name: this.name,
        };

        const url = endpoints.databases.aiAgent.aiAgentStart + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to start AI agent", response.responseText, response.statusText)
        );
    }
}

export = startAiAgentCommand;
