import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseActivateCommand extends commandBase {

    constructor(private licensePayload: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.adminLicenseActivate;

        return this.post(url, JSON.stringify(this.licensePayload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                const message = response.status === 405 ?
                    "License activation has been disabled on this server" : 
                    "Failed to activate license";

                this.reportError(message, response.responseText, response.statusText);
            });
    }
}

export = licenseActivateCommand; 
