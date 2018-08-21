import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCertificatesCommand extends commandBase {

    constructor(private includeSecondary: boolean = false) {
        super();
    }
    
    execute(): JQueryPromise<{ Certificates: Array<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>, LoadedServerCert: string, WellKnownAdminCerts: Array<string> }> {
        const args = {
            secondary: this.includeSecondary
        };
        const url = endpoints.global.adminCertificates.adminCertificates + this.urlEncodeArgs(args);
        
        return this.query(url, null, null, x => ({ Certificates: x.Results, LoadedServerCert: x.LoadedServerCert, WellKnownAdminCerts: x.WellKnownAdminCerts }))
            .fail((response: JQueryXHR) => this.reportError("Unable to get list of certificates", response.responseText, response.statusText));
    }
}

export = getCertificatesCommand;
