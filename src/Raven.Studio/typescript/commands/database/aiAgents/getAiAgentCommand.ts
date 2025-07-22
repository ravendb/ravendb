import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

interface ResultDto {
    AiAgents: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration[];
}

class GetAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId?: string
    ) {
        super();
    }

    execute(): JQueryPromise<ResultDto> {
        const args = {
            agentId: this.agentId,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.query<ResultDto>(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError(
                "Failed to get AI " + this.agentId ? "agent" : "agents",
                response.responseText,
                response.statusText
            )
        );
    }
}

export = GetAiAgentCommand;
