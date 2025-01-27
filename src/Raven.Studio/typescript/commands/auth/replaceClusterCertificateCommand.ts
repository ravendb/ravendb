import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");

class replaceClusterCertificateCommand extends commandBase {

    constructor(private dto: certificatesTypes.ReplaceServerCertificateDto, private isReplaceImmediately: boolean) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const args = {
            replaceImmediately: this.isReplaceImmediately
        };

        const url = endpoints.global.adminCertificates.adminCertificatesReplaceClusterCert + this.urlEncodeArgs(args);
        
        return this.post<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess("The server certificate(s) will be replaced when all the nodes confirm receipt. An alert will be raised upon success."))
            .fail((response: JQueryXHR) => this.reportError("Unable to replace server certificate", response.responseText, response.statusText));
    }
}

export = replaceClusterCertificateCommand;
