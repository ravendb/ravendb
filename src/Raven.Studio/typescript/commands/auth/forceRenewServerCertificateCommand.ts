import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class forceRenewServerCertificateCommand extends commandBase {
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificatesLetsencryptForceRenew;  
                
        return this.post<void>(url, null)
            .done(() => this.reportSuccess("A renew request for this server certificate was created."))
            .fail((response: JQueryXHR) => this.reportError("Failed to renew the server certificate", response.responseText, response.statusText));
    }
}

export = forceRenewServerCertificateCommand;
