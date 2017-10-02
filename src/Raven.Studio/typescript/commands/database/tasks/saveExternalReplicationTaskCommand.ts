import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveExternalReplicationTaskCommand extends commandBase {
   
    private externalReplicationToSend: Raven.Client.ServerWide.ExternalReplication;

    constructor(private db: database, private taskId: number, private replicationSettings: externalReplicationDataFromUI) {
        super();

        this.externalReplicationToSend = {
            // From UI:
            Name: replicationSettings.TaskName,
            Database: replicationSettings.DestinationDB,
            Url: replicationSettings.DestinationURL,
            MentorNode: replicationSettings.MentorNode,
            // Other vals:
            TaskId: taskId
        } as Raven.Client.ServerWide.ExternalReplication;
    }
 
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        return this.updateReplication()
            .fail((response: JQueryXHR) => {
                if (this.taskId === 0) {
                    this.reportError("Failed to create replication task for: " + this.replicationSettings.DestinationDB, response.responseText, response.statusText);
                } else {
                    this.reportError("Failed to save replication task", response.responseText, response.statusText);
                }
            })
            .done(() => {
                if (this.taskId === 0) {
                    this.reportSuccess(
                        `Created replication task from database ${this.db.name} to ${this.replicationSettings.DestinationDB}`);
                } else {
                    this.reportSuccess(`Updated replication task`);
                }
            });
    }

    private updateReplication(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {

        const url = endpoints.databases.ongoingTasks.adminTasksExternalReplication;
        
        const addRepTask = $.Deferred<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult>();

        const payload = {          
            Watcher: this.externalReplicationToSend
        };

        this.post(url, JSON.stringify(payload), this.db)
            .done((results: Array<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult>) => {
                addRepTask.resolve(results[0]);
            })
            .fail(response => addRepTask.reject(response));

        return addRepTask;
    }
}

export = saveExternalReplicationTaskCommand; 

