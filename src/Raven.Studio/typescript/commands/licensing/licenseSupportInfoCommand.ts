import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseSupportInfoCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseSupportInfo> {
        const url = endpoints.global.license.licenseSupport;

        return this.query(url, null, null, x => x)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get license support info", response.responseText, response.statusText);
            });
    }
}

export = licenseSupportInfoCommand; 
