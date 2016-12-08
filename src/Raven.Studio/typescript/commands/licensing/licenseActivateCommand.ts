import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseActivateCommand extends commandBase {

    constructor(private licensePayload: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.licenseActivate;

        return this.post(url, JSON.stringify(this.licensePayload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to activate license", response.responseText, response.statusText);
            });
    }
}

export = licenseActivateCommand; 
