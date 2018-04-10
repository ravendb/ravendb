import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getRestorePointsCommand extends commandBase {

    constructor(private path: string, private skipReportingError: boolean) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints> {
        const url = endpoints.global.adminDatabases.adminRestorePoints;
        const args = {
            Path: this.path
        };
        return this.post(url, JSON.stringify(args))
            .fail((response: JQueryXHR) => {
                if (this.skipReportingError) {
                    return;
                }

                this.reportError(`Failed to get restore points for path: ${this.path}`,
                    response.responseText,
                    response.statusText);
            });
    }
}

export = getRestorePointsCommand; 
