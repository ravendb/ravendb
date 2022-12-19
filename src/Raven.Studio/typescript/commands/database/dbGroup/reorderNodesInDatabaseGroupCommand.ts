import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class reorderNodesInDatabaseGroupCommand extends commandBase {

    private readonly databaseName: string;
    private readonly nodesOrder: string[];
    private readonly fixedTopology: boolean;

    constructor(databaseName: string, nodesOrder: string[], fixedTopology: boolean) {
        super();
        this.fixedTopology = fixedTopology;
        this.nodesOrder = nodesOrder;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.databaseName,
        };
        const url = endpoints.global.adminDatabases.adminDatabasesReorder + this.urlEncodeArgs(args);

        const payload: Raven.Client.ServerWide.Operations.ReorderDatabaseMembersOperation.Parameters = {
            MembersOrder: this.nodesOrder,
            Fixed: this.fixedTopology
        };
        
        return this.post<void>(url, JSON.stringify(payload), null,  { dataType: undefined })
            .done(() => this.reportSuccess("Nodes order was successfully saved"))
            .fail((response: JQueryXHR) => this.reportError("Failed to change nodes order", response.responseText, response.statusText));
    }
}

export = reorderNodesInDatabaseGroupCommand;
