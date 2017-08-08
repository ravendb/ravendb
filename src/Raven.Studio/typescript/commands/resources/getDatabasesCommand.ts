import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabasesCommand extends commandBase {

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        const url = endpoints.global.databases.databases;

        return this.query(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to load databases", response.responseText, response.statusText));
    }
}

export = getDatabasesCommand;
