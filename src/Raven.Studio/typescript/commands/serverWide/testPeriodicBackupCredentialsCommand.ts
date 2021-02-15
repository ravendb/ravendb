import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class testPeriodicBackupCredentialsCommand extends commandBase {
    constructor(private type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType,
                private connectionConfiguration: Raven.Client.Documents.Operations.Backups.BackupSettings) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.studioTasks.studioTasksPeriodicBackupTestCredentials +
            this.urlEncodeArgs({
                type: this.type
            });

        return this.post(url, JSON.stringify(this.connectionConfiguration), null, { dataType: undefined })
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test connection`, result.Error);
                }
            })
            .fail(response => this.reportError(`Connection to ${this.type} failed`,
                    response.responseText, response.statusText));
    }
}

export = testPeriodicBackupCredentialsCommand; 

