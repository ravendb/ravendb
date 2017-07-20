import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class promoteClusterNodeCommand extends commandBase {

    constructor(private nodeTag: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag
        }
        const url = endpoints.global.rachisAdmin.adminClusterPromote + this.urlEncodeArgs(args);

        return this.post<void>(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to promote node", response.responseText));
    }
}

export = promoteClusterNodeCommand;
