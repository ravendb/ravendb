import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class backupNowCommand extends commandBase {
    constructor(private db: database, private taskId: number, private isFullBackup: boolean) {
        super();
    }
 
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.ongoingTasks.adminBackupDatabase +
            this.urlEncodeArgs({
                taskId: this.taskId,
                isFullBackup: this.isFullBackup
            });

        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => this.reportSuccess(`Successfully started a backup task`))
            .fail(response => {
                this.reportError(`Failed to start a backup for task id: ${this.taskId}`,
                    response.responseText,
                    response.statusText);
            });
    }
}

export = backupNowCommand; 

