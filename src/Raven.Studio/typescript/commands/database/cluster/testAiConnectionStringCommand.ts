import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

type Settings =
    | Raven.Client.Documents.Operations.AI.OpenAiSettings
    | Raven.Client.Documents.Operations.AI.AzureOpenAiSettings
    | Raven.Client.Documents.Operations.AI.OllamaSettings
    | Raven.Client.Documents.Operations.AI.OnnxSettings
    | Raven.Client.Documents.Operations.AI.GoogleSettings
    | Raven.Client.Documents.Operations.AI.HuggingFaceSettings
    | Raven.Client.Documents.Operations.AI.MistralAiSettings;

class testAiConnectionStringCommand extends commandBase {
    constructor(
        private db: database | string,
        private type: Raven.Client.Documents.Operations.AI.AiConnectorType,
        private settings: Partial<Settings>
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            type: this.type,
        };

        const url = endpoints.databases.aiIntegrationConnection.adminAiTestConnection + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.settings), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError(`Failed to test AI connection`, response.responseText, response.statusText)
            )
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test AI connection`, result.Error);
                }
            });
    }
}

export = testAiConnectionStringCommand;
