import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabaseCommand extends commandBase {

    constructor(private dbName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.DatabaseInfo> {
        const url = endpoints.global.resources.resource;
        const args = {
            name: this.dbName
        };

        return this.query<Raven.Client.Server.Operations.DatabaseInfo>(url, args);
    }
}

export = getDatabaseCommand;
