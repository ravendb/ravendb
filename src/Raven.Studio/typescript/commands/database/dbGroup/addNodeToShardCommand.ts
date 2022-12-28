import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addNodeToShardCommand extends commandBase {

    constructor(private databaseName: string, private nodeTagToAdd: string, private mentorNode: string = undefined) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult> {
        const args = {
            databaseName: this.databaseName,
            node: this.nodeTagToAdd,
            mentor: this.mentorNode
        };
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesShard + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult>(url, null)
            .done(() => this.reportSuccess("Node " + this.nodeTagToAdd + " was added to shard " + this.databaseName))
            .fail((response: JQueryXHR) => this.reportError("Failed to add node to shard", response.responseText, response.statusText));
    }
}

export = addNodeToShardCommand;
