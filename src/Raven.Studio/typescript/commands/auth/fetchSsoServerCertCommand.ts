import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");

class fetchSsoServerCertCommand extends commandBase {

    constructor(private url: string) {
        super();
    }

    execute(): JQueryPromise<certificatesTypes.FetchSsoServerCertResult> {
        const url = endpoints.global.adminCertificates.adminCertificatesSsoServerFetch;

        return this.query<certificatesTypes.FetchSsoServerCertResult>(url, { url: this.url })
            .fail((response: JQueryXHR) => this.reportError("Unable to fetch certificate from URL", response.responseText, response.statusText));
    }
}

export = fetchSsoServerCertCommand;
