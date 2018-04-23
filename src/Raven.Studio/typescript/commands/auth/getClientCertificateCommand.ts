import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClientCertificateCommand extends commandBase {

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition> {
        const url = endpoints.global.adminCertificates.certificatesWhoami;
        
        return this.query<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get client certificate from server", response.responseText,  response.statusText));
    }
}

export = getClientCertificateCommand;
