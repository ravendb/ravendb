import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseSupportInfoCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseSupportInfo> {
        const url = endpoints.global.license.licenseSupport;

        return this.post(url, null, null)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get license support info", response.responseText, response.statusText);
            });
    }
}

export = licenseSupportInfoCommand; 
