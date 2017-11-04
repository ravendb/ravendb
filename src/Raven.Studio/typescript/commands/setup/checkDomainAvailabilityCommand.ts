import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class checkDomainAvailabilityCommand extends commandBase {

    constructor(private domainName: string) {
        super();
    }

    execute(): JQueryPromise<boolean> {
        const args = {
            domain: this.domainName
        };
        const url = endpoints.global.setup.setupCheckDomain + this.urlEncodeArgs(args);

        return this.query(url, null, null, x => x.Available)
            .fail((response: JQueryXHR) => this.reportError("Failed to check domain availability", response.responseText, response.statusText));
    }
}

export = checkDomainAvailabilityCommand;
