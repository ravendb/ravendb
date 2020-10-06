import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerWideBackupCommand extends commandBase {
    constructor(private backupName: string) {
        super();
    }

    // Return specific server-wide Backup task by its name
    execute(): JQueryPromise<Raven.Server.Web.System.ServerWideTasksResult<Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration>> {
        const args = {
            type: "Backup",
            name: this.backupName
        };
        
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideTasks + this.urlEncodeArgs(args);

        const deferred = $.Deferred<Raven.Server.Web.System.ServerWideTasksResult<Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration>>();

        this.query<Raven.Server.Web.System.ServerWideTasksResult<Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration>>(url, null)
            .done((result: Raven.Server.Web.System.ServerWideTasksResult<Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration>) => deferred.resolve(result))
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get Server-Wide Backup: ${this.backupName}`, response.responseText, response.statusText);
                deferred.reject();
            });

        return deferred;
    }
}

export = getServerWideBackupCommand;
