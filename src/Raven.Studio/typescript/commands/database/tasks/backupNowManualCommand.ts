import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class backupNowManualCommand extends commandBase {
    constructor(private db: database, private manualBackupDto: Raven.Client.Documents.Operations.Backups.BackupConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Backups.StartBackupOperationResult> {
        const url = endpoints.databases.ongoingTasks.adminBackup;
        
        const payload = this.manualBackupDto;

        return this.post(url, JSON.stringify(payload), this.db)
            .done(() => this.reportSuccess("A manual backup has been started successfully"))
            .fail(response => this.reportError("Failed to start the manual backup", response.responseText, response.statusText));
    }
}

export = backupNowManualCommand;