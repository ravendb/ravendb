import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabasesCommand extends commandBase {

    execute(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        const url = endpoints.global.resources.resources;

        return this.query(url, null);
    }
}

export = getDatabasesCommand;
