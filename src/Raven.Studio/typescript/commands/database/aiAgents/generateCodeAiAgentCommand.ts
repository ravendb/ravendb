import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export type AiAgentGenerateCodeLanguage = "c#" | "python" | "javascript";

export interface AiAgentGenerateCodeResultDto {
    GeneratedCode: string;
}

export default class generateCodeAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId: string,
        private language: AiAgentGenerateCodeLanguage
    ) {
        super();
    }

    execute() {
        const args = {
            agentId: this.agentId,
            language: this.language,
        };

        const url = endpoints.databases.aiAgent.adminAiAgentGenerateCode;

        return this.query<AiAgentGenerateCodeResultDto>(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to generate code for AI agent", response.responseText, response.statusText)
        );
    }
}
