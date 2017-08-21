import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deactivateLicenseCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.adminLicenseDeactivate;

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to deactivate license", response.responseText, response.statusText);
            });
    }
}

export = deactivateLicenseCommand; 
