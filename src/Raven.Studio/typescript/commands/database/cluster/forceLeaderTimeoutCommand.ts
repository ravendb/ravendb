import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class forceLeaderTimeoutCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.rachisAdmin.adminClusterTimeout;

        return this.post<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess("Timeout was forced"))
            .fail((response: JQueryXHR) => this.reportError("Failed to force timeout", response.responseText));
    }
}

export = forceLeaderTimeoutCommand;
