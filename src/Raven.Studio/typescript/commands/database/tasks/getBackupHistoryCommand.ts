import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getBackupHistoryCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<BackupHistoryResponse> {
        const url = endpoints.databases.backupHistory.backupHistory;

        return this.query<BackupHistoryResponse>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get backup history", response.responseText, response.statusText));
    }
}

export = getBackupHistoryCommand; 
