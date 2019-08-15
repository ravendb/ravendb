import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteServerWideBackupCommand extends commandBase {
    constructor(private taskName: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const args = {
            name: this.taskName
        };
        const url = endpoints.global.adminServerWideBackup.adminConfigurationServerWideBackup + this.urlEncodeArgs(args);

        return this.del<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse>(url, null)
            .done(() => this.reportSuccess(`Successfully deleted Server-Wide Backup: ${this.taskName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete Server-Wide Backup: ${this.taskName}`, response.responseText));
    }
}

export = deleteServerWideBackupCommand;
