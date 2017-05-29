import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand extends commandBase {

    constructor(private db: database, private taskType: Raven.Server.Web.System.OngoingTaskType, private taskId: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get info for task of type: " + this.taskType, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Successfully retrieved information for task of type: ${this.taskType}`);
            });
    }

    private getTaskInfo(): JQueryPromise<Raven.Client.Server.Operations.ModifyExternalReplicationResult> {

        // TODO: Call the dedicated ep...!!!
       
        return $.Deferred<Raven.Client.Server.Operations.ModifyExternalReplicationResult>();
    }
}

export = getOngoingTaskInfoCommand; 