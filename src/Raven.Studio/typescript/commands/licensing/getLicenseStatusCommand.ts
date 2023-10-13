import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getLicenseStatusCommand extends commandBase {

    execute(): JQueryPromise<LicenseStatus> {
        const url = endpoints.global.license.licenseStatus;
        return this.query<LicenseStatus>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get license status", response.responseText));
    }
}

export = getLicenseStatusCommand;
