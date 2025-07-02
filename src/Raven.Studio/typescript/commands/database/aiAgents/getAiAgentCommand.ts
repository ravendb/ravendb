import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type AiAgentResult<TName extends string | undefined> = TName extends string
    ? Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
    : Record<string, Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>;

class GetAiAgentCommand<TName extends string | undefined = undefined> extends commandBase {
    constructor(
        private db: string,
        private name?: TName
    ) {
        super();
    }

    execute(): JQueryPromise<AiAgentResult<TName>> {
        const args = {
            name: this.name,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.query<AiAgentResult<TName>>(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to get AI agent", response.responseText, response.statusText)
        );
    }
}

export = GetAiAgentCommand;
