import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteExternalReplicationTaskCommand extends commandBase {
    
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
       
        // TODO: Call the dedicated ep...!!!
        alert("Delete is not implemented yet..");
        return $.Deferred<Raven.Client.Server.Operations.ModifyExternalReplicationResult>();;
    }
}

export = deleteExternalReplicationTaskCommand; 