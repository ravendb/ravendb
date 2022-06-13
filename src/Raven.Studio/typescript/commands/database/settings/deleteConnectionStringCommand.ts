import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import ongoingTaskModel from "models/database/tasks/ongoingTaskModel";

class deleteConnectionStringCommand extends commandBase {
    
    constructor(private db: database, private type: StudioEtlType, private connectionStringName: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            type: ongoingTaskModel.getServerEtlType(this.type),
            connectionString: this.connectionStringName
        };
        
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(`Successfully deleted connection string - ${this.connectionStringName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete connection string - ${this.connectionStringName}`, response.responseText, response.statusText));
    }
}

export = deleteConnectionStringCommand;
