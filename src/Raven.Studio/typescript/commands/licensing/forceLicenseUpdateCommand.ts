import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class forceLicenseUpdateCommand extends commandBase {

    execute(): JQueryPromise<boolean> {
        const url = endpoints.global.license.adminLicenseForceUpdate;
        return this.post(url, null, null)
            .done(() => this.reportSuccess("Your license was successfully updated"))
            .fail((response: JQueryXHR) => {
                const message = response.status === 405 ?
                    "License activation has been disabled on this server" :
                    "Failed to activate license";

                this.reportError(message, response.responseText, response.statusText);
            });
    }
}

export = forceLicenseUpdateCommand;
