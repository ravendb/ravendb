import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class demoteClusterNodeCommand extends commandBase {

    constructor(private nodeTag: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag
        }
    
        const url = endpoints.global.rachisAdmin.adminClusterDemote + this.urlEncodeArgs(args);

        return this.post<void>(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to demote node", response.responseText));
    }
}

export = demoteClusterNodeCommand;
