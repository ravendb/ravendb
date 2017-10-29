import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class backupNowCommand extends commandBase {
    constructor(private db: database, private taskId: number, private isFullBackup: boolean) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.adminDatabases.adminBackupDatabase +
            this.urlEncodeArgs({
                name: this.db.name,
                taskId: this.taskId,
                isFullBackup: this.isFullBackup
            });

        return this.post(url, null, null, { dataType: undefined })
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to start a backup for task id: ${this.taskId}`, result.Error);
                }
            })
            .fail(response => this.reportError(`Failed to start a backup for task id: ${this.taskId}`,
                    response.responseText, response.statusText));
    }
}

export = backupNowCommand; 

