import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId?: string,
        private abortSignal?: AbortSignal
    ) {
        super();
    }

    execute(): Promise<GetAiAgentResultDto> {
        const args = {
            agentId: this.agentId,
        };

        const relativeUrl = endpoints.databases.aiAgent.adminAiAgent + this.urlEncodeArgs(args);

        return this.fetch({
            relativeUrl,
            db: this.db,
            options: {
                method: "GET",
                signal: this.abortSignal,
            },
        })
            .then(async (response) => {
                if (!response.ok) {
                    const responseText = await response.text();
                    this.reportError(this.getErrorTitle(), responseText, response.statusText);

                    throw new Error(responseText || response.statusText);
                }

                return response.json();
            })
            .catch((error) => {
                if (error instanceof Error && error.name === "AbortError") {
                    throw error;
                }

                this.reportError(this.getErrorTitle(), error instanceof Error ? error.message : "Unknown error");
                throw error;
            });
    }

    private getErrorTitle(): string {
        if (this.agentId) {
            return `Failed to get AI agent with ID: ${this.agentId}`;
        }
        return "Failed to get AI agents";
    }
}

export = getAiAgentCommand;
