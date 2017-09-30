import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class savePeriodicBackupConfigurationCommand extends commandBase {
    constructor(private db: database, private configuration: Raven.Client.ServerWide.PeriodicBackup.PeriodicBackupConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackup + this.urlEncodeArgs({ name: this.db.name });

        const isNewTask = this.configuration.TaskId === 0;
        return this.post(url, JSON.stringify(this.configuration))
            .done((results: Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult) => {
                const taskTypeText = isNewTask ? "created" : "updated";
                this.reportSuccess(`Succefully ${taskTypeText} backup configuration with task ID: ${results.TaskId}`);
            })
            .fail(response => {
                const errorMessage = isNewTask ?
                    "Failed to save a new periodic backup task" :
                    `Failed to save a periodic backup task with id ${this.configuration.TaskId}`;
                this.reportError(errorMessage, response.responseText, response.statusText);
            });
    }
}

export = savePeriodicBackupConfigurationCommand; 

