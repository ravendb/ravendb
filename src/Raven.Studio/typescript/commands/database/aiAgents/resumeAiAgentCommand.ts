import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class resumeAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private name: string,
        private chatId: string,
        private dto: Raven.Client.Documents.Operations.AI.Agents.ResumeChatBody
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.AI.Agents.ChatResult<object>> {
        const args = {
            name: this.name,
            chatId: this.chatId,
        };

        const url = endpoints.databases.aiAgent.aiAgentResume + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to resume AI agent", response.responseText, response.statusText)
        );
    }
}

export = resumeAiAgentCommand;
