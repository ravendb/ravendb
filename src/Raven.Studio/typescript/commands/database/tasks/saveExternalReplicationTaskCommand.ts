import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveOngoingTasksCommand extends commandBase {

    private replicationTasksToSend: Array<Raven.Client.Server.DatabaseWatcher> = [];

    constructor(private newRepTask: externalReplicationDataFromUI, private db: database, private taskId?: number) {
        super();

        const replicatonWatcherObject: Raven.Client.Server.DatabaseWatcher = {
            // From UI:
            ApiKey: newRepTask.ApiKey,
            Database: newRepTask.DestinationDB,
            Url: newRepTask.DestinationURL,
            // Other vals:
            ClientVisibleUrl: null,
            Disabled: false,
            Humane: null,
            IgnoredClient: false,
            NodeTag: null,
            SpecifiedCollections: null,
            TransitiveReplicationBehavior: null,
            CurrentTaskId: taskId
        };
        this.replicationTasksToSend.push(replicatonWatcherObject);
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {
        return this.addReplication()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to create replication task for: " + this.newRepTask.DestinationDB, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Replication task From: ${this.db.name} To: ${this.newRepTask.DestinationDB} was created`);
            });
    }

    private addReplication(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {
        const url = endpoints.global.adminDatabases.adminModifyWatchers + this.urlEncodeArgs({ name: this.db.name });
        // Just for now... to be changed later to a dedicated ep...

        const addRepTask = $.Deferred<Raven.Client.Server.Operations.ModifyExternalReplicationResult>();

        const payload = {
            Watchers: this.replicationTasksToSend
        };

        this.post(url, JSON.stringify(payload))
            .done((results: Array<Raven.Client.Server.Operations.ModifyExternalReplicationResult>) => {
                addRepTask.resolve(results[0]);
            })
            .fail(response => addRepTask.reject(response));

        return addRepTask;
    }
}

export = saveOngoingTasksCommand; 

