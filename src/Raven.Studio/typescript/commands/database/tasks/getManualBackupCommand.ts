import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getManualBackupCommand extends commandBase {

    constructor(private dbName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult> {

        const args = {
            taskId: 0,
            name: this.dbName
        }
        
        const url = endpoints.global.backupDatabase.periodicBackupStatus;

        return this.query<Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult>(url, args)
            .fail((response: JQueryXHR) => this.reportError("Failed to get info about the manual backup", response.responseText, response.statusText));
    }
}

export = getManualBackupCommand;
