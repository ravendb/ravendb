import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");

class fetchSsoServerCertCommand extends commandBase {

    constructor(private url: string) {
        super();
    }

    execute(): JQueryPromise<certificatesTypes.FetchSsoServerCertResult> {
        const endpointUrl = endpoints.global.adminCertificates.adminCertificatesSsoServerFetch + this.urlEncodeArgs({ url: this.url });

        return this.ajax<certificatesTypes.FetchSsoServerCertResult>(endpointUrl, null, "GET")
            .fail((response: JQueryXHR) => this.reportError("Unable to fetch certificate from URL", response.responseText, response.statusText));
    }
}

export = fetchSsoServerCertCommand;
