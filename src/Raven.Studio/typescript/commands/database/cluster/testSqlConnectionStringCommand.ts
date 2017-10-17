import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class testSqlConnectionStringCommand extends commandBase {

    constructor(private db: database, private connectionString: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            factoryName: "System.Data.SqlClient" //TODO: only MsSQL is supported for now 
        };
        const url = endpoints.databases.sqlEtl.adminEtlSqlTestConnection + this.urlEncodeArgs(args);

        return this.post(url, this.connectionString, this.db)
            .fail((response: JQueryXHR) => this.reportError(`Failed to test SQL connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test SQL connection`, result.Error);
                }
            });
    }
}

export = testSqlConnectionStringCommand;
