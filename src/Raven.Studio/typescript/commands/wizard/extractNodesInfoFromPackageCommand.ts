import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class extractNodesInfoFromPackageCommand extends commandBase {

    constructor(private zipContents: string) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Server.Web.System.ConfigurationNodeInfo>> {
        const url = endpoints.global.setup.setupContinueExtract;
        const payload: Partial<Raven.Server.Commercial.ContinueSetupInfo> = {
            Zip: this.zipContents,
        };

        return this.post<Raven.Server.Web.System.ConfigurationNodeInfo>(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError("Failed to fetch configuration parameters", response.responseText, response.statusText));
    }
}

export = extractNodesInfoFromPackageCommand;
