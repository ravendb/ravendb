import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConnectionStringInfoCommand extends commandBase {
    constructor(private db: database, private type: Raven.Client.ServerWide.ConnectionStringType, private connectionStringName: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.ServerWide.ETL.RavenConnectionString | Raven.Client.ServerWide.ETL.SqlConnectionString> {
        return this.getConnectionStringInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for connection string: ${this.connectionStringName}`, response.responseText, response.statusText);
            });
    }

    private getConnectionStringInfo(): JQueryPromise<Raven.Client.ServerWide.ETL.RavenConnectionString | Raven.Client.ServerWide.ETL.SqlConnectionString> { 
        const args = { name: this.db.name, type: this.type, connectionStringName: this.connectionStringName };
        const url = endpoints.global.adminDatabases.adminConnectionString + this.urlEncodeArgs(args);

        return this.query<any>(url, null);
    }
}

export = getConnectionStringInfoCommand; 