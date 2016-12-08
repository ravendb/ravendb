import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseRegistrationCommand extends commandBase {

    constructor(private registrationData: Raven.Server.Commercial.UserRegistrationInfo) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.licenseRegistration;

        return this.post(url, JSON.stringify(this.registrationData), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send registration information", response.responseText, response.statusText);
            });
    }
}

export = licenseRegistrationCommand; 
