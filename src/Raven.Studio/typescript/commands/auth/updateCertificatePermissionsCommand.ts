import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificateModel = require("models/auth/certificateModel");

class updateCertificatePermissionsCommand extends commandBase {

    constructor(private model: certificateModel) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const deleteExistingConfiguration = this.model.mode() === "editExisting" && this.model.twoFactorActionOnEdit() === "delete";
        
        const url = endpoints.global.adminCertificates.adminCertificatesEdit + this.urlEncodeArgs({
            deleteTwoFactorConfiguration: deleteExistingConfiguration ? true : undefined
        });
        
        const payload = this.model.toUpdatePermissionsDto();
        return this.post<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess("Certificate permissions were updated successfully"))
            .fail((response: JQueryXHR) => this.reportError("Unable to update certificate permissions", response.responseText, response.statusText));
    }
}

export = updateCertificatePermissionsCommand;
