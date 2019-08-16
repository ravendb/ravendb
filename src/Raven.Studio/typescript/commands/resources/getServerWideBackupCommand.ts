import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerWideBackupCommand extends commandBase {
    constructor(private backupName: string) {
        super();
    }
 
    // Return specific server-wide Backup task by its name
    execute(): JQueryPromise<Raven.Server.Web.System.ServerWideBackupConfigurationResults> {
        const url = endpoints.global.adminServerWideBackup.adminConfigurationServerWideBackup + this.urlEncodeArgs({ name: this.backupName});

        return this.query<Raven.Server.Web.System.ServerWideBackupConfigurationResults>(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to get Server-Wide Backup :${this.backupName}`, response.responseText, response.statusText));
    }
}

export = getServerWideBackupCommand;
