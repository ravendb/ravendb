import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testPeriodicBackupCredentialsCommand extends commandBase {
    constructor(private db: database,
        private type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType,
        private connectionConfiguration: Raven.Client.ServerWide.PeriodicBackup.BackupSettings) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackupTestCredentials +
            this.urlEncodeArgs({
                name: this.db.name,
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

