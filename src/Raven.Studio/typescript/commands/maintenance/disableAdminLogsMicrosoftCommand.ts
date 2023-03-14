import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class disableAdminLogsMicrosoftCommand extends commandBase {
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLogsMicrosoftDisable;

        return this.post<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess("Microsoft logs was successfully disabled"))
            .fail((response: JQueryXHR) => this.reportError("Failed to disable Microsoft logs", response.responseText, response.statusText));
    }
}

export = disableAdminLogsMicrosoftCommand;
