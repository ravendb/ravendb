import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getLicenseConfigurationSettingsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Web.Studio.LicenseConfigurationSettings> {
        const url = endpoints.global.license.licenseConfiguration;
        
        return this.query<Raven.Server.Web.Studio.LicenseConfigurationSettings>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get license configuration settings", response.responseText));
    }
}

export = getLicenseConfigurationSettingsCommand;
