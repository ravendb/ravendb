import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addNodeToClusterCommand extends commandBase {

    constructor(private serverUrl: string, private addAsWatcher: boolean, private assignedCores?: number, private maxUtilizedCores?: number, private nodeTag?: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            url: this.serverUrl,
            watcher: this.addAsWatcher,
            assignedCores: this.assignedCores,
            maxUtilizedCores: this.maxUtilizedCores,
            tag: this.nodeTag,
        };
        const url = endpoints.global.rachisAdmin.adminClusterNode + this.urlEncodeArgs(args);

        return this.put<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess(`Successfully added node to cluster`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to add node to cluster`, response.responseText, response.statusText));
    }
}

export = addNodeToClusterCommand;
