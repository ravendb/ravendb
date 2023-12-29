import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class forgotTwoFactorSecretCommand extends commandBase {
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.twoFactorAuthentication.authentication2fa;
        
        return this.del<void>(url, undefined, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Unable to log out from 2FA session", response.responseText, response.statusText));
    }
}

export = forgotTwoFactorSecretCommand;
