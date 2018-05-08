import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class savePeriodicBackupConfigurationCommand extends commandBase {
    constructor(private db: database, private configuration: Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const url = endpoints.databases.ongoingTasks.adminPeriodicBackup;

        const isNewTask = this.configuration.TaskId === 0;
        return this.post(url, JSON.stringify(this.configuration), this.db)
            .done((results: Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult) => {
                const taskTypeText = isNewTask ? "created" : "updated";
                this.reportSuccess(`Succefully ${taskTypeText} backup configuration`);
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

