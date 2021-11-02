import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getConnectivityToLicenseServerCommand extends commandBase {
 
    execute(): JQueryPromise<Raven.Server.Web.Studio.LicenseHandler.ConnectivityToLicenseServer> {
        const url = endpoints.global.license.licenseServerConnectivity;

        return this.query<any>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError("No connection to RavenDB License Server", response.responseText, response.statusText);
            });
    }
}

export = getConnectivityToLicenseServerCommand;
