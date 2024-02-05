import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getTwoFactorServerConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Server.Web.Authentication.TwoFactorAuthenticationHandler.TotpServerConfiguration> {
        const url = endpoints.global.twoFactorAuthentication.authentication2faConfiguration;
        
        return this.query<Raven.Server.Web.Authentication.TwoFactorAuthenticationHandler.TotpServerConfiguration>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to load two factor server configuration", response.responseText, response.statusText));
    }
}

export = getTwoFactorServerConfigurationCommand;
