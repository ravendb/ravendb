import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");

class registerSsoServerCertCommand extends commandBase {

    constructor(private dto: certificatesTypes.RegisterSsoServerCertDto) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificates;

        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess("SSO server certificate was registered successfully"))
            .fail((response: JQueryXHR) => this.reportError("Unable to register SSO server certificate", response.responseText, response.statusText));
    }
}

export = registerSsoServerCertCommand;
