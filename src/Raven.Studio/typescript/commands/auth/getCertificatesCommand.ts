import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

interface Result {
    Certificates: Array<
        Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition & { HasTwoFactor: boolean }
    >;
    LoadedServerCert: string;
    LoadedServerCertForCommunication?: string;
    WellKnownAdminCerts: string[];
    WellKnownIssuers: string[];
}

class getCertificatesCommand extends commandBase {
    constructor(private includeSecondary: boolean = false, private metadataOnly: boolean = true) {
        super();
    }

    execute(): JQueryPromise<Result> {
        const args = {
            secondary: this.includeSecondary,
            metadataOnly: this.metadataOnly,
        };
        const url = endpoints.global.adminCertificates.adminCertificates + this.urlEncodeArgs(args);

        return this.query(url, null, null, (x) => ({
            Certificates: x.Results,
            LoadedServerCert: x.LoadedServerCert,
            LoadedServerCertForCommunication: x.LoadedServerCertForCommunication,
            WellKnownAdminCerts: x.WellKnownAdminCerts,
            WellKnownIssuers: x.WellKnownIssuers,
        })).fail((response: JQueryXHR) =>
            this.reportError("Unable to get list of certificates", response.responseText, response.statusText)
        );
    }
}

export = getCertificatesCommand;
