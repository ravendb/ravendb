import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCertificatesCommand extends commandBase {

    private readonly includeSecondary: boolean;
    private readonly metadataOnly: boolean;

    constructor(includeSecondary: boolean = false, metadataOnly: boolean = true) {
        super();
        this.includeSecondary = includeSecondary;
        this.metadataOnly = metadataOnly;
    }
    
    execute(): JQueryPromise<CertificatesResponseDto> {
        const args = {
            secondary: this.includeSecondary,
            metadataOnly: this.metadataOnly
        };
        const url = endpoints.global.adminCertificates.adminCertificates + this.urlEncodeArgs(args);
        
        return this.query(url, null, null, x => ({ Certificates: x.Results, LoadedServerCert: x.LoadedServerCert, WellKnownAdminCerts: x.WellKnownAdminCerts, WellKnownIssuers: x.WellKnownIssuers }))
            .fail((response: JQueryXHR) => this.reportError("Unable to get list of certificates", response.responseText, response.statusText));
    }
}

export = getCertificatesCommand;
