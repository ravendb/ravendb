import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class reorderNodesInDatabaseGroupCommand extends commandBase {

    constructor(private databaseName: string, private nodesOrder: string[], private fixedTopology: boolean) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.databaseName,
        };
        const url = endpoints.global.adminDatabases.adminDatabasesReorder + this.urlEncodeArgs(args);

        const payload = {
            MembersOrder: this.nodesOrder,
            Fixed: this.fixedTopology
        } as Raven.Client.ServerWide.Operations.ReorderDatabaseMembersOperation.Parameters;
        
        return this.post<void>(url, JSON.stringify(payload), null,  { dataType: undefined })
            .done(() => this.reportSuccess("Nodes order was successfully saved"))
            .fail((response: JQueryXHR) => this.reportError("Failed to change nodes order", response.responseText, response.statusText));
    }
}

export = reorderNodesInDatabaseGroupCommand;
