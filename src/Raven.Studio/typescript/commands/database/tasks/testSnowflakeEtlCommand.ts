import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testSnowflakeReplicationCommand<TRelationalConnectionString extends Raven.Client.Documents.Operations.ConnectionStrings.ConnectionString, TRelationalEtlConfiguration extends Raven.Client.Documents.Operations.ETL.EtlConfiguration<TRelationalConnectionString>> extends commandBase {
    constructor(private db: database | string, private payload: Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.TestRelationalDatabaseEtlScript<TRelationalConnectionString, TRelationalEtlConfiguration>) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test.RelationalDatabaseEtlTestScriptResult> {
        const url = endpoints.databases.snowflakeEtl.adminEtlSnowflakeTest;

        return this.post<Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test.RelationalDatabaseEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to test Snowflake ETL`, response.responseText, response.statusText);
            });
    }
}

export = testSnowflakeReplicationCommand; 

