import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private name: string,
        private dto: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
    ) {
        super();
    }

    execute(): JQueryPromise<any> {
        const args = {
            name: this.name,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent + this.urlEncodeArgs(args);

        return this.put(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to save AI agent", response.responseText, response.statusText)
        );
    }
}

export = saveAiAgentCommand;
