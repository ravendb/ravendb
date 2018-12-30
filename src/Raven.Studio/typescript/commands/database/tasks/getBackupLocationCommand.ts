import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getBackupLocationCommand extends commandBase {

    constructor(private inputPath: string, private database: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.DataDirectoryResult> {
        const args = {
            path: this.inputPath,
            getNodesInfo: true
        };

        const url = endpoints.databases.ongoingTasks.adminBackupDataDirectory + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.Studio.DataDirectoryResult>(url, null, this.database)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to calculate the full backup path", response.responseText, response.statusText));
    }
}

export = getBackupLocationCommand;
