import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteConnectionStringCommand extends commandBase {

    constructor(private db: database, private type: Raven.Client.ServerWide.ConnectionStringType, private connectionStringName: string) {
        super();
    }

    execute(): JQueryPromise<void> {                       
        const args = { name: this.db.name, type: this.type, connectionString: this.connectionStringName };
        const url = endpoints.global.adminDatabases.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.del<void>(url, null)
            .done(() => this.reportSuccess(`Successfullly deleted connection string - ${this.connectionStringName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete connection string - ${this.connectionStringName}`, response.responseText, response.statusText));
    }
}

export = deleteConnectionStringCommand;