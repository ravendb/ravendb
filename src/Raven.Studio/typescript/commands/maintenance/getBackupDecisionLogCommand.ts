import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getBackupDecisionLogCommand extends commandBase {
    constructor(
        private databaseName?: string,
        private take?: number
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.ServerWide.Backups.BackupDecisionLogDetails> {
        const url = endpoints.global.backupDatabase.adminDebugPeriodicBackupDecisionLog;

        const args = {
            database: this.databaseName || undefined,
            take: this.take || undefined,
        };

        return this.query<Raven.Server.ServerWide.Backups.BackupDecisionLogDetails>(url, args).fail(
            (response: JQueryXHR) =>
                this.reportError("Failed to load the backup decision log", response.responseText, response.statusText)
        );
    }
}

export = getBackupDecisionLogCommand;
