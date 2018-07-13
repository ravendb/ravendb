import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testRavenEtlCommand extends commandBase {
    constructor(private db: database, private payload: Raven.Server.Documents.ETL.Providers.Raven.Test.TestRavenEtlScript) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.Raven.Test.RavenEtlTestScriptResult> {
        const url = endpoints.databases.ravenEtl.adminEtlRavenTest;

        return this.post<Raven.Server.Documents.ETL.Providers.Raven.Test.RavenEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(`Failed to test Raven ETL`, response.responseText, response.statusText);
            });
    }
}

export = testRavenEtlCommand; 

