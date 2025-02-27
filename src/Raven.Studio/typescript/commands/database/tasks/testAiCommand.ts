import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testAiCommand extends commandBase {
    constructor(
        private db: database | string,
        private payload: Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test.TestEmbeddingsGenerationScript
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test.EmbeddingsGenerationTestScriptResult> {
        const url = endpoints.databases.aiIntegrationConnection.adminAiTestConnection;

        return this.post<Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test.EmbeddingsGenerationTestScriptResult>(url, JSON.stringify(this.payload), this.db).fail((response: JQueryXHR) => {
            this.reportError(`Failed to test AI`, response.responseText, response.statusText);
        });
    }
}

export = testAiCommand;
