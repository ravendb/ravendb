import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getBackupLocationCommand extends commandBase {

    constructor(private inputPath: string) {
        super();
    }

    execute(): JQueryPromise<Array<string>> {
        const args = {
            path: this.inputPath
        };

        const url =  endpoints.global.studioTasks.adminStudioTasksFullDataDirectory + this.urlEncodeArgs(args);

        return this.query<Array<string>>(url, null, null, x => x.PathDetails)
            .fail((response: JQueryXHR) => this.reportError("Failed to calculate the backup full path", response.responseText, response.statusText));
    }
}

export = getBackupLocationCommand;
