import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificateModel = require("models/auth/certificateModel");

class uploadCertificateCommand extends commandBase {

    constructor(private model: certificateModel) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificates;
        
        const payload = this.model.toUploadCertificateDto();
        return this.put<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess("Certificate was saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Unable to upload certificate", response.responseText, response.statusText));
    }
}

export = uploadCertificateCommand;
