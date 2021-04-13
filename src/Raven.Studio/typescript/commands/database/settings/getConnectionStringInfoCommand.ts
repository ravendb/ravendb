import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConnectionStringInfoCommand extends commandBase {
    private constructor(private db: database, private type: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType, private connectionStringName: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult> {
        return this.getConnectionStringInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for connection string: ${this.connectionStringName}`, response.responseText, response.statusText);
            });
    }

    private getConnectionStringInfo(): JQueryPromise<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult> {
        const args = { connectionStringName: this.connectionStringName, type: this.type };
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.query(url, null, this.db);
    }

    static forRavenEtl(db: database, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Raven", connectionStringName);
    }

    static forSqlEtl(db: database, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Sql", connectionStringName);
    }

    static forOlapEtl(db: database, connectionStringName: string) {
        return new getConnectionStringInfoCommand(db, "Olap", connectionStringName);
    }
}

export = getConnectionStringInfoCommand; 
