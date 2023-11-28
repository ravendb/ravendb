import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class generateTwoFactorSecretCommand extends commandBase {

    execute(): JQueryPromise<{ Secret: string  }> {
        const url = endpoints.global.adminCertificates.adminCertificates2faGenerate;
        
        return this.query<{ Secret: string }>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to generate authentication key", response.responseText, response.statusText));
    }
}

export = generateTwoFactorSecretCommand;
