import commandBase = require("commands/commandBase");
import backupDecisionLogTypes = require("components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogTypes");
import BackupDecisionLogDetails = backupDecisionLogTypes.BackupDecisionLogDetails;

class getBackupDecisionLogCommand extends commandBase {
    constructor(
        private databaseName?: string,
        private take?: number
    ) {
        super();
    }

    execute(): JQueryPromise<BackupDecisionLogDetails> {
        const url =
            "/admin/debug/periodic-backup/decision-log" +
            this.urlEncodeArgs({ database: this.databaseName || undefined, take: this.take || undefined });

        return this.query<BackupDecisionLogDetails>(url, null, undefined);
    }
}

export = getBackupDecisionLogCommand;
