import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class extractNodesInfoFromPackageCommand extends commandBase {

    constructor(private zipContents: string) {
        super();
    }

    execute(): JQueryPromise<void> { //TODO: change return type!
        const url = endpoints.global.setup.setupContinueExtract;
        const payload = {
            Zip: this.zipContents
        } as Raven.Server.Commercial.ContinueSetupInfo;

        return this.post<void>(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError("Failed to fetch configuration parameters", response.responseText, response.statusText));
    }
}

export = extractNodesInfoFromPackageCommand;
