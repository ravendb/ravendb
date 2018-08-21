import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testSqlReplicationCommand extends commandBase {
    constructor(private db: database, private payload: Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters.TestSqlEtlScript) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.SQL.Test.SqlEtlTestScriptResult> {
        const url = endpoints.databases.sqlEtl.adminEtlSqlTest;

        return this.post<Raven.Server.Documents.ETL.Providers.SQL.Test.SqlEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(`Failed to test SQL replication`, response.responseText, response.statusText);
            });
    }
}

export = testSqlReplicationCommand; 

