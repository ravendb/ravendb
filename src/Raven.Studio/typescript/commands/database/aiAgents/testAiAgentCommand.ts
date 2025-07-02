import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class testAiAgentCommand extends commandBase {

    constructor(private db: string, private dto: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration & {Parameters: TODO; Prompt: string}) {
        super();
    }

    execute(): JQueryPromise<TODO> {
        const url = endpoints.databases.aiAgent.aiAgentTest;

        // TODO handle error
        return this.post(url, JSON.stringify(this.dto), this.db);
    }
}

export = testAiAgentCommand;
