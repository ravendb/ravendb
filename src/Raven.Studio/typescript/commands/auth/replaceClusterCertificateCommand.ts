import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificateModel = require("models/auth/certificateModel");

class replaceClusterCertificateCommand extends commandBase {

    constructor(private model: certificateModel) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const args = {
            replaceImmediately: this.model.replaceImmediately()
        };
        const url = endpoints.global.adminCertificates.adminCertificatesReplaceClusterCert + this.urlEncodeArgs(args);
        
        const payload = this.model.toReplaceCertificateDto();
        
        return this.post<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess("The server certificate(s) will be replaced when all the nodes confirm receipt. An alert will be raised upon success."))
            .fail((response: JQueryXHR) => this.reportError("Unable to replace server certificate", response.responseText, response.statusText));
    }
}

export = replaceClusterCertificateCommand;
