import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId: string
    ) {
        super();
    }

    execute() {
        const args = {
            agentId: this.agentId,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent + this.urlEncodeArgs(args);

        return this.del(url, null, this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to delete AI agent", response.responseText, response.statusText)
        );
    }
}

export = deleteAiAgentCommand;
