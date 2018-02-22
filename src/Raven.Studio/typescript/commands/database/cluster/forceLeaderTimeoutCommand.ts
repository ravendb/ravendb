import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class forceLeaderTimeoutCommand extends commandBase {

    constructor(private targetNode: string) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.rachisAdmin.adminClusterTimeout;

        return this.post<void>(url, null, null, { dataType: undefined }, 9000, this.targetNode)
            .done(() => this.reportSuccess("Timeout was forced"))
            .fail((response: JQueryXHR) => this.reportError("Failed to force timeout", response.responseText));
    }
}

export = forceLeaderTimeoutCommand;
