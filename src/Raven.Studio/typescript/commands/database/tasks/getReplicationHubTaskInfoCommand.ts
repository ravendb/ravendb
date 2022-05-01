import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { shardingTodo } from "common/developmentHelper";

class getReplicationHubTaskInfoCommand extends commandBase {

    constructor(private db: database, private taskId: number) {
          super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections> {
        
        shardingTodo("ANY", "Currently, the Hub GET is implemented only for Single-Shard-Only (while PUT is All-Shards-Only)");
        const location = this.db.getFirstLocation(clusterTopologyManager.default.localNodeTag());
        
        const args = {
            key: this.taskId,
            ...location
        };
        
        const url = endpoints.databases.ongoingTasks.tasksPullReplicationHub + this.urlEncodeArgs(args);
        
        return this.query<Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for Replication Hub task with id: ${this.taskId}. `, response.responseText, response.statusText);
            });
    }
}

export = getReplicationHubTaskInfoCommand; 
