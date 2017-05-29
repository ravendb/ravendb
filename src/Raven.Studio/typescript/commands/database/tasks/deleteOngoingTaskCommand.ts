import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteExternalReplicationTaskCommand extends commandBase {
    
    private replicationTasksToSend: Array<Raven.Client.Server.DatabaseWatcher> = []; 

    constructor(private db: database, private taskType: Raven.Server.Web.System.OngoingTaskType, private taskId: number) {
        super();
       
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> { 
        return this.deleteTask()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to delete task of type: " + this.taskType, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Task of deleted From: ${this.db.name}`);
            });
    }

    private deleteTask(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {
       
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

export = deleteExternalReplicationTaskCommand; 