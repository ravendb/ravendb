import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addShardToDatabaseGroupCommand extends commandBase {

    constructor(private databaseName: string, private replicationFactor: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult> {
        const args = {
            name: this.databaseName,
            replicationFactor: this.replicationFactor,
        };
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesShard + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult>(url, null)
            .done(() => this.reportSuccess("New shard was created"))
            .fail((response: JQueryXHR) => this.reportError("Failed to create new shard", response.responseText, response.statusText));
    }
}

export = addShardToDatabaseGroupCommand;
