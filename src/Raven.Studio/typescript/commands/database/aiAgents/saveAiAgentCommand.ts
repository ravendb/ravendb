import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
    ) {
        super();
    }

    execute() {
        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.put(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to save AI agent", response.responseText, response.statusText)
        );
    }
}

export = saveAiAgentCommand;
