import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getBackupHistoryDetailsCommand extends commandBase {

    constructor(private db: database, private taskId: number, private id: string) {
          super();
    }

    execute(): JQueryPromise<{ Details: Raven.Client.Documents.Operations.Backups.BackupResult }> {
        const url = endpoints.databases.backupHistory.backupHistoryDetails;
        
        const args = {
            taskId: this.taskId,
            id: this.id, 
        }

        return this.query<{ Details: Raven.Client.Documents.Operations.Backups.BackupResult }>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get backup history details", response.responseText, response.statusText));
    }
}

export = getBackupHistoryDetailsCommand; 
