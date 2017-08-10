import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConnectionStringInfoCommand<T extends Raven.Client.ServerWide.ETL.RavenConnectionString | Raven.Client.ServerWide.ETL.SqlConnectionString> extends commandBase {
    private constructor(private db: database, private type: Raven.Client.ServerWide.ConnectionStringType, private connectionStringName: string) {
        super();
    }
    
    execute(): JQueryPromise<T> {
        return this.getConnectionStringInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for connection string: ${this.connectionStringName}`, response.responseText, response.statusText);
            });
    }

    private getConnectionStringInfo(): JQueryPromise<T> { 
        const args = { name: this.db.name, type: this.type, connectionStringName: this.connectionStringName };
        const url = endpoints.global.adminDatabases.adminConnectionString + this.urlEncodeArgs(args);

        return this.query<T>(url, null);
    }

    static forRavenEtl(db: database, connectionStringName: string) {
        return new getConnectionStringInfoCommand<Raven.Client.ServerWide.ETL.RavenConnectionString>(db, "Raven", connectionStringName);
    }

    static forSqlEtl(db: database, connectionStringName: string) {
        return new getConnectionStringInfoCommand<Raven.Client.ServerWide.ETL.SqlConnectionString>(db, "Sql", connectionStringName);
    }
}

export = getConnectionStringInfoCommand; 