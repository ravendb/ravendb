import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAllServerWideBackupTasksCommand extends commandBase {
 
    execute(): JQueryPromise<Raven.Server.Web.System.ServerWideBackupConfigurationResults> {
        const url = endpoints.global.adminServerWideBackup.adminConfigurationServerWideBackup;

        return this.query<Raven.Server.Web.System.ServerWideBackupConfigurationResults>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get all Server-Wide backup tasks`,
                    response.responseText, response.statusText);
            });
    }
}

export = getAllServerWideBackupTasksCommand;
