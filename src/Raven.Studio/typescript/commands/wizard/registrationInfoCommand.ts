import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class registrationInfoCommand extends commandBase {

    constructor(private license: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Commercial.UserDomainsAndLicenseInfo> {
        const url = endpoints.global.setup.setupUserDomains;
        const payload = {
            License: this.license
        };

        const task = $.Deferred<Raven.Server.Commercial.UserDomainsAndLicenseInfo>();

        this.post(url, JSON.stringify(payload), null)
            .done(result => task.resolve(result))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to complete domain registration", response.responseText, response.statusText);
                task.reject();
            });
        
        return task;
    }
    
    private static tryExtractError(payload: string) {
        try {
            const json = JSON.parse(payload);
            if (json && json.Error) {
                return json.Error;
            }
        } catch (e) {
        }
        return payload;
    }
}

export = registrationInfoCommand;
