import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addNodeToDatabaseGroupCommand extends commandBase {

    constructor(private databaseName: string, private nodeTagToAdd: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.DatabasePutResult> {
        const args = {
            name: this.databaseName,
            node: this.nodeTagToAdd
        };
        const url = endpoints.global.adminDatabases.adminDatabasesAddNode + this.urlEncodeArgs(args);

        return this.post(url, null)
            .done(() => this.reportSuccess("Node " + this.nodeTagToAdd +" was added to database group " + this.databaseName))
            .fail((response: JQueryXHR) => this.reportError("Failed to add node to database group", response.responseText, response.statusText));
    }
}

export = addNodeToDatabaseGroupCommand;
