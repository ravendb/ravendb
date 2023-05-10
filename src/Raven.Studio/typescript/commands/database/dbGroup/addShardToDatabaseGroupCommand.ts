import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addShardToDatabaseGroupCommand extends commandBase {

    private readonly databaseName: string;

    private readonly replicationFactor: number;
    
    private readonly nodes: string[];

    constructor(databaseName: string, replicationFactor: number, nodes: string[]) {
        super();
        this.replicationFactor = replicationFactor;
        this.databaseName = databaseName;
        this.nodes = nodes;
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult> {
        const args = {
            name: this.databaseName,
            replicationFactor: this.nodes ? undefined : this.replicationFactor,
            node: this.nodes ?? undefined
        };
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesShard + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult>(url, null)
            .done(() => this.reportSuccess("New shard was created"))
            .fail((response: JQueryXHR) => this.reportError("Failed to create new shard", response.responseText, response.statusText));
    }
}

export = addShardToDatabaseGroupCommand;
