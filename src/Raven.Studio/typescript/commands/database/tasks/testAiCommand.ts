import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testAiCommand extends commandBase {
    constructor(
        private db: database | string,
        private payload: Raven.Server.Documents.ETL.Providers.AI.Test.TestAiIntegrationScript
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.AI.Test.AiIntegrationTestScriptResult> {
        const url = endpoints.databases.aiIntegration.adminAiTest;

        return this.post<Raven.Server.Documents.ETL.Providers.AI.Test.AiIntegrationTestScriptResult>(url, JSON.stringify(this.payload), this.db).fail((response: JQueryXHR) => {
            this.reportError(`Failed to test AI`, response.responseText, response.statusText);
        });
    }
}

export = testAiCommand;
