import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificateModel = require("models/auth/certificateModel");

class uploadCertificateCommand extends commandBase {

    constructor(private model: certificateModel) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Server.Commercial.LicenseStatus> {
        const url = endpoints.global.adminCertificates.adminCertificates;
        
        const payload = this.model.toUploadCertificateDto();
        return this.post(url, JSON.stringify(payload), null)
            .fail((response: JQueryXHR) => this.reportError("Unable to upload certificate", response.responseText, response.statusText));
    }
}

export = uploadCertificateCommand;
