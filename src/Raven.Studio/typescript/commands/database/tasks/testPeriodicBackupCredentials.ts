import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testPeriodicBackupCredentials extends commandBase {
    constructor(private db: database,
        private type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType,
        private connectionConfiguration: any) {
        super();
    }
 
    execute(): JQueryPromise<any> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackupTestCredentials +
            this.urlEncodeArgs({
                name: this.db.name,
                type: this.type
            });

        const task = $.Deferred<any>();

        this.post(url, JSON.stringify(this.connectionConfiguration))
            .done(() => {
                this.reportSuccess(`Connection to ${this.type} was successful`);
                task.resolve();
            })
            .fail(response => {
                this.reportError(`Connection to ${this.type} failed`,
                    response.responseText, response.statusText);
                task.reject(response);
            });

        return task;
    }
}

export = testPeriodicBackupCredentials; 

