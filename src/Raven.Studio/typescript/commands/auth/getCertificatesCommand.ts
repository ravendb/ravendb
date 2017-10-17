import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCertificatesCommand extends commandBase {

    constructor(private includeSecondary: boolean = false) {
        super();
    }
    
    execute(): JQueryPromise<Array<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>> {
        const args = {
            secondary: this.includeSecondary
        };
        const url = endpoints.global.adminCertificates.adminCertificates + this.urlEncodeArgs(args);
        
        return this.query(url, null, null, x => x.Results)
            .fail((response: JQueryXHR) => this.reportError("Unable to get list of certificates", response.responseText, response.statusText));
    }
}

export = getCertificatesCommand;
