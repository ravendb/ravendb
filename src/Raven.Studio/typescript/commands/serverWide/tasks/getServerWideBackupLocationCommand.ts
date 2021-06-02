import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerWideBackupLocationCommand extends commandBase {
    
    constructor(private inputPath: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.DataDirectoryResult> {
        const args = {
            path: this.inputPath,
            getNodesInfo: true
        };

        const url = endpoints.global.adminStudioServerWide.adminServerWideBackupDataDirectory + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.Studio.DataDirectoryResult>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to calculate the full backup path", response.responseText, response.statusText));
    }
}

export = getServerWideBackupLocationCommand;
