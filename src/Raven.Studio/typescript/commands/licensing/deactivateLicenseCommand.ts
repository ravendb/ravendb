import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deactivateLicenseCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.adminLicenseDeactivate;

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                let message;
                if (response.status == 405) {
                    message = "License deactivation feature has been disabled on this server";
                } else {
                    message = "Failed to deactivate license";
                }

                this.reportError(message, response.responseText, response.statusText);
            });
    }
}

export = deactivateLicenseCommand; 
