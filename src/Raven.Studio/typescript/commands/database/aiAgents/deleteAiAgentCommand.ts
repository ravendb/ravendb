import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private name: string
    ) {
        super();
    }

    execute() {
        const args = {
            name: this.name,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.del(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to get AI agent", response.responseText, response.statusText)
        );
    }
}

export = deleteAiAgentCommand;
