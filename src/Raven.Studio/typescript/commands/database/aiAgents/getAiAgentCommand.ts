import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type AiAgentConfiguration = Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;

class GetAiAgentCommand<TId extends string | undefined = undefined> extends commandBase {
    constructor(
        private db: string,
        private id?: TId
    ) {
        super();
    }

    execute(): JQueryPromise<AiAgentConfiguration[]> {
        const args = {
            id: this.id,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.query<AiAgentConfiguration[]>(url, args, this.db).then((result: unknown) => {
            if (this.id) {
                return [result as AiAgentConfiguration];
            }
            return (result as { AiAgents: AiAgentConfiguration[] }).AiAgents;
        }).fail((response: JQueryXHR) =>
            this.reportError("Failed to get AI agent", response.responseText, response.statusText)
        );
    }
}

export = GetAiAgentCommand;
