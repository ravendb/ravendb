import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getLicenseLimitsUsage extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseLimitsUsage> {
        const url = endpoints.global.license.licenseLimitsUsage;
        
        return this.query<Raven.Server.Commercial.LicenseLimitsUsage>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get license limits usage", response.responseText));
    }
}

export = getLicenseLimitsUsage;
