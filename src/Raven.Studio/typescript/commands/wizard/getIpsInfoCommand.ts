import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getIpsInfoCommand extends commandBase {

    constructor(private selectedRootDomain: string, private userDomainsWithIps: Raven.Server.Commercial.UserDomainsWithIps) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Commercial.UserDomainsWithIps> {

        const args = {
            rootDomain: this.selectedRootDomain
        };
        
        const url = endpoints.global.setup.setupPopulateIps + this.urlEncodeArgs(args);
        
        const payload = {
            Emails: this.userDomainsWithIps.Emails,
            RootDomains: this.userDomainsWithIps.RootDomains,
            Domains: this.userDomainsWithIps.Domains
        };

        const task = $.Deferred<Raven.Server.Commercial.UserDomainsWithIps>();

        this.post(url, JSON.stringify(payload), null)
            .done(result => task.resolve(result))
            .fail((response: JQueryXHR) => {
                if (response.status === 404) {
                    task.resolve(null);
                } else {
                    this.reportError("Failed to get ips information", response.responseText, response.statusText);
                    task.reject();
                }
            });
        
        return task;       
    }
}

export = getIpsInfoCommand;
