import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addNodeToDatabaseGroupCommand extends commandBase {

    constructor(private databaseName: string, private nodeTagToAdd: string, private mentorNode: string = undefined) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        const args = {
            name: this.databaseName,
            node: this.nodeTagToAdd,
            mentor: this.mentorNode
        };
        const url = endpoints.global.adminDatabases.adminDatabasesNode + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Operations.DatabasePutResult>(url, null)
            .done(() => this.reportSuccess("Node " + this.nodeTagToAdd + " was added to database group " + this.databaseName))
            .fail((response: JQueryXHR) => this.reportError("Failed to add node to database group", response.responseText, response.statusText));
    }
}

export = addNodeToDatabaseGroupCommand;
