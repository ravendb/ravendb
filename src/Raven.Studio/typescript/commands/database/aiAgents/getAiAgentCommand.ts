import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAiAgentCommand extends commandBase {

    constructor(private db: string, private name?: string) {
        super();
    }

    execute(): JQueryPromise<Record<string, Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>> {
        const args = {
            name: this.name,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent;

        // TODO handle error
        return this.query(url, args, this.db)
    }
}

export = getAiAgentCommand;
