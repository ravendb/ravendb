import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getRestorePointsCommand extends commandBase {

    constructor(private path: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.PeriodicBackup.RestorePoints> {
        const url = endpoints.global.adminDatabases.adminGetRestorePoints;
        const args = {
            Path: this.path
        };
        return this.post(url, JSON.stringify(args))
            .fail((response: JQueryXHR) => this.reportError(`Failed to get restore points for path: ${this.path}`,
                response.responseText, response.statusText));
    }
}

export = getRestorePointsCommand; 
