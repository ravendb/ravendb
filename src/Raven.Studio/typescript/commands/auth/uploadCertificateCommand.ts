import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");

class uploadCertificateCommand extends commandBase {

    constructor(private dto: certificatesTypes.UploadCertificateDto) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificates;
        
        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess("Certificate was saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Unable to upload certificate", response.responseText, response.statusText));
    }
}

export = uploadCertificateCommand;
