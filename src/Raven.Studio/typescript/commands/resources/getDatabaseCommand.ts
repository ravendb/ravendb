import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabaseCommand extends commandBase {

    constructor(private dbName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabaseInfo> {
        const url = endpoints.global.databases.databases;
        const args = {
            name: this.dbName
        };

        return this.query<Raven.Client.ServerWide.Operations.DatabaseInfo>(url, args);
    }
}

export = getDatabaseCommand;
