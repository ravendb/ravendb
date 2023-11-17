import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteConnectionStringCommand extends commandBase {
    private readonly db: database;
    private readonly type: Raven.Client.Documents.Operations.ETL.EtlType;
    private readonly connectionStringName: string;

    constructor(db: database, type: Raven.Client.Documents.Operations.ETL.EtlType, connectionStringName: string) {
        super();
        this.db = db;
        this.type = type;
        this.connectionStringName = connectionStringName;
    }

    execute(): JQueryPromise<void> {
        const args = {
            type: this.type,
            connectionString: this.connectionStringName
        };
        
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(`Successfully deleted connection string - ${this.connectionStringName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete connection string - ${this.connectionStringName}`, response.responseText, response.statusText));
    }
}

export = deleteConnectionStringCommand;
