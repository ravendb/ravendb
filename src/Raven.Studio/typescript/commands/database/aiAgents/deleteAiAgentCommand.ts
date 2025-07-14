import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private id: string
    ) {
        super();
    }

    execute() {
        const args = {
            id: this.id,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.del(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to delete AI agent", response.responseText, response.statusText)
        );
    }
}

export = deleteAiAgentCommand;
