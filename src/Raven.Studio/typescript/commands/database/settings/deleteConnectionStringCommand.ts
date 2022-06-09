import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteConnectionStringCommand extends commandBase {

    connectionStringType: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType;
    
    constructor(private db: database, private type: StudioEtlType, private connectionStringName: string) {
        super();
        
        if (type === "Kafka" || type === "RabbitMQ") {
            this.connectionStringType = "Queue";
        } else {
            this.connectionStringType = type;
        }
    }

    execute(): JQueryPromise<void> {
        const args = { type: this.connectionStringType, connectionString: this.connectionStringName };
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(`Successfully deleted connection string - ${this.connectionStringName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete connection string - ${this.connectionStringName}`, response.responseText, response.statusText));
    }
}

export = deleteConnectionStringCommand;
