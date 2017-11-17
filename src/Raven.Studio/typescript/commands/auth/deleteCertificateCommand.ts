import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteCertificateCommand extends commandBase {

    constructor(private certificateThumbprint: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Server.Commercial.LicenseStatus> {
        const args = {
            thumbprint: this.certificateThumbprint
        };
        const url = endpoints.global.adminCertificates.adminCertificates + this.urlEncodeArgs(args);
        
        return this.del<Raven.Server.Commercial.LicenseStatus>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError("Unable to delete certificate: " + this.certificateThumbprint, response.responseText, response.statusText));
    }
}

export = deleteCertificateCommand;
