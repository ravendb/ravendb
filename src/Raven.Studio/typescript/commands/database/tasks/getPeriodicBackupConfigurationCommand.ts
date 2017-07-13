import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getPeriodicBackupConfigurationCommand extends commandBase {
    constructor(private db: database, private taskId: number) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Server.PeriodicBackup.PeriodicBackupConfiguration> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackup +
            this.urlEncodeArgs({ name: this.db.name, taskId: this.taskId });

        const getTask = this.query(url, null);
        getTask.fail((response: JQueryXHR) => {
            this.reportError(`Failed to get periodic backup configuration for task: ${this.taskId}`,
                response.responseText, response.statusText);
        });
        return this.query(url, null);
    }
}

export = getPeriodicBackupConfigurationCommand; 

