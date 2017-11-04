import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class registrationInfoCommand extends commandBase {

    constructor(private license: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<registrationInfoResult> {
        const url = endpoints.global.setup.setupRegistrationInfo;
        const payload = {
            License: this.license
        };

        return this.post(url, JSON.stringify(payload), null)
            .fail((response: JQueryXHR) => this.reportError("Failed to load registration information", response.responseText, response.statusText));
    }
}

export = registrationInfoCommand;
