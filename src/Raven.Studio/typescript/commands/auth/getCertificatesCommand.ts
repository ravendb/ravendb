import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificateModel = require("models/auth/certificateModel");

class getCertificatesCommand extends commandBase {

    execute(): JQueryPromise<Array<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>> {
        const url = endpoints.global.adminCertificates.adminCertificates;
        
        return this.query(url, null, null, x => x.Results)
            .fail((response: JQueryXHR) => this.reportError("Unable to get list of certificates", response.responseText, response.statusText));
    }
}

export = getCertificatesCommand;
