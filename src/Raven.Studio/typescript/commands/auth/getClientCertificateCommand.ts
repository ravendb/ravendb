import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClientCertifiateCommand extends commandBase {

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition> {
        const url = endpoints.global.adminCertificates.certificatesWhoami;
        return this.query(url, null, null);
    }

}

export = getClientCertifiateCommand;
