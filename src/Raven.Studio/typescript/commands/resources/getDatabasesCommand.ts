import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabasesCommand extends commandBase {

    execute(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        const url = endpoints.global.databases.databases;

        return this.query(url, null);
    }
}

export = getDatabasesCommand;
