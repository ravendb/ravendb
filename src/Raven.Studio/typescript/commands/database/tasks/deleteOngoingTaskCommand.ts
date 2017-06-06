import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteOngoingTaskCommand extends commandBase {
    
    constructor(private db: database, private taskType: Raven.Client.Server.Operations.OngoingTaskType, private taskId: number) {
        super();
    }

    execute(): JQueryPromise<void> { 
        switch (this.taskType) {
            case "Replication":
                return this.deleteWatcher();
            default:
                //TODO: handle other task types (Use single ep for delete - see issue 7241)
                throw new Error("Not yet implemented");
        }
    }

    private deleteWatcher() {
        const args = {
            name: this.db.name,
            id: this.taskId
        };

        const url = endpoints.global.ongoingTasks.adminTasksDelete + this.urlEncodeArgs(args);

        return this.post<void>(url, null);
    }
}

export = deleteOngoingTaskCommand; 