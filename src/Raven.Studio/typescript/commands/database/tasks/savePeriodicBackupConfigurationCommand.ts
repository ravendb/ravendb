import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class savePeriodicBackupConfigurationCommand extends commandBase {
    constructor(private db: database, private configuration: Raven.Client.Server.PeriodicBackup.PeriodicBackupConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyOngoingTaskResult> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackupUpdate + this.urlEncodeArgs({ name: this.db.name });
        const updatePeriodicBackupTask = $.Deferred<Raven.Client.Server.Operations.ModifyOngoingTaskResult>();

        const isNewTask = this.configuration.TaskId === 0;
        this.post(url, JSON.stringify(this.configuration))
            .done((results: Raven.Client.Server.Operations.ModifyOngoingTaskResult) => {
                var taskTypeText = isNewTask ? "created" : "updated";
                this.reportSuccess(`Succefully ${taskTypeText} backup configuration with task ID: ${results.TaskId}`);
                updatePeriodicBackupTask.resolve(results);
            })
            .fail(response => {
                var taskText = isNewTask ? "new task" : this.configuration.TaskId;
                this.reportError(`Failed to save the periodic backup configuration for task: ${taskText}`,
                    response.responseText, response.statusText);
                updatePeriodicBackupTask.reject(response);
            });

        return updatePeriodicBackupTask;
    }
}

export = savePeriodicBackupConfigurationCommand; 

