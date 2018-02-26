import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClusterDomainsCommand extends commandBase {

    execute(): JQueryPromise<Array<string>> {
        const url = endpoints.global.adminCertificates.adminCertificatesClusterDomains;
        
        return this.query<Array<string>>(url, null, null,  x => x.ClusterDomains)
            .fail((response: JQueryXHR) => this.reportError("Failed to get cluster domains", response.responseText,  response.statusText));
    }
}

export = getClusterDomainsCommand;
