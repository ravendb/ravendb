import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class enableAdminLogsMicrosoftCommand extends commandBase {
    
    execute(): JQueryPromise<string> {
        const url = endpoints.global.adminLogs.adminLogsMicrosoftEnable;

        return this.post<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess("Microsoft logs was successfully enabled"))
            .fail((response: JQueryXHR) => this.reportError("Failed to enable Microsoft logs", response.responseText, response.statusText));
    }
}

export = enableAdminLogsMicrosoftCommand;
