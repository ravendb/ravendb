import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId?: string
    ) {
        super();
    }

    execute(): JQueryPromise<GetAiAgentResultDto> {
        const args = {
            agentId: this.agentId,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.query<GetAiAgentResultDto>(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError(
                "Failed to get AI " + this.agentId ? "agent" : "agents",
                response.responseText,
                response.statusText
            )
        );
    }
}

export = getAiAgentCommand;
