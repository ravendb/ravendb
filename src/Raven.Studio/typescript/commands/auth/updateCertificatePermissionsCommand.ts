import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificateModel = require("models/auth/certificateModel");

class updateCertificatePermissionsCommand extends commandBase {

    constructor(private model: certificateModel) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificatesEdit; 
        
        const payload = this.model.toUpdatePermissionsDto();
        return this.post<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess("Certificate permissions were updated successfully"))
            .fail((response: JQueryXHR) => this.reportError("Unable to update certificate permissions", response.responseText, response.statusText));
    }
}

export = updateCertificatePermissionsCommand;
