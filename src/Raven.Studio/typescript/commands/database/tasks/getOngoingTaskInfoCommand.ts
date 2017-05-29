import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand extends commandBase {

    private replicationTasksToSend: Array<Raven.Client.Server.DatabaseWatcher> = [];

    constructor(private db: database, private taskType: Raven.Server.Web.System.OngoingTaskType, private taskId: number) {
        super();

        //const replicatonWatcherObject: Raven.Client.Server.DatabaseWatcher = {
        //    // From UI:
        //    ApiKey: newRepTask.ApiKey,
        //    Database: newRepTask.DestinationDB,
        //    Url: newRepTask.DestinationURL,
        //    // Other vals:
        //    ClientVisibleUrl: null,
        //    Disabled: false,
        //    Humane: null,
        //    IgnoredClient: false,
        //    NodeTag: null,
        //    SpecifiedCollections: null,
        //    TransitiveReplicationBehavior: null,
        //    CurrentTaskId: taskId
        //};
        //this.replicationTasksToSend.push(replicatonWatcherObject);
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get info for task of type: " + this.taskType, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Info retrieved successfully for task of type: ${this.taskType}`);
            });
    }

    private getTaskInfo(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {

        // TODO: Change to the dedicated ep...!!

        const url = endpoints.global.adminDatabases.adminModifyWatchers + this.urlEncodeArgs({ name: this.db.name });

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

export = getOngoingTaskInfoCommand; 