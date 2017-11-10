import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class loadAgreementCommand extends commandBase {

    constructor(private email: string) {
        super();
    }

    execute(): JQueryPromise<string> {
        const args = {
            email: this.email
        };
        
        const url = endpoints.global.setup.setupLetsencryptAgreement + this.urlEncodeArgs(args);

        return this.query(url, null, null, x => x.Uri)
            .fail((response: JQueryXHR) => this.reportError("Failed to load Let's Encrypt agreement", response.responseText, response.statusText));
    }
}

export = loadAgreementCommand;
