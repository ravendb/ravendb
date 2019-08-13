import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteServerWideBackupCommand extends commandBase {
    constructor(private taskName: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Server.Web.System.ModifyServerWideBackupResult> {
        const args = {
            name: this.taskName
        };
        const url = endpoints.global.backupServerWide.adminConfigurationServerWideBackup + this.urlEncodeArgs(args);

        return this.del<Raven.Server.Web.System.ModifyServerWideBackupResult>(url, null)
            .done(() => this.reportSuccess(`Successfully deleted Server-Wide Backup: ${this.taskName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete Server-Wide Backup: ${this.taskName}`, response.responseText));
    }
}

export = deleteServerWideBackupCommand;
