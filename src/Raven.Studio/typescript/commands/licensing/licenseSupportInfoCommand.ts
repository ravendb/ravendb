import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseSupportInfoCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseSupportInfo> {
        const url = endpoints.global.license.licenseSupport;

        return this.query<Raven.Server.Commercial.LicenseSupportInfo>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get license support info", response.responseText, response.statusText);
            });
    }
}

export = licenseSupportInfoCommand; 
