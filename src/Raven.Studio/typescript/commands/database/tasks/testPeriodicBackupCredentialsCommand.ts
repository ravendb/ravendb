import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testPeriodicBackupCredentialsCommand extends commandBase {
    constructor(private db: database,
        private type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType,
        private connectionConfiguration: Raven.Client.Server.PeriodicBackup.BackupSettings) {
        super();
    }
 
    execute(): JQueryPromise<any> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackupTestCredentials +
            this.urlEncodeArgs({
                name: this.db.name,
                type: this.type
            });

        return this.post(url, JSON.stringify(this.connectionConfiguration))
            .done(() => this.reportSuccess(`Connection to ${this.type} was successful`))
            .fail(response => this.reportError(`Connection to ${this.type} failed`,
                    response.responseText, response.statusText));
    }
}

export = testPeriodicBackupCredentialsCommand; 

