import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export default class testServerWideAiConnectionStringCommand extends commandBase {
    constructor(
        private type: Raven.Client.Documents.Operations.AI.AiConnectorType,
        private modelType: Raven.Client.Documents.Operations.AI.AiModelType,
        private settings: Partial<AiConnectionStringsSettings>
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            type: this.type,
            modelType: this.modelType,
        };

        const url =
            endpoints.global.aiIntegrationServerWideConnection.adminAiTestConnection + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.settings))
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
