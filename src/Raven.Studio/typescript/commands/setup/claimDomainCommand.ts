import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class claimDomainCommand extends commandBase {

    constructor(private domain: string, private license: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<registrationInfoResult> {
        const url = endpoints.global.setup.setupDnsNCert; //TODO: 
        const payload = { 
            Domain: this.domain,
            License: this.license
        } as Raven.Server.Commercial.ClaimDomainInfo;

        return this.post(url, JSON.stringify(payload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to obtain domain information", response.responseText, response.statusText));
    }
}

export = claimDomainCommand;
