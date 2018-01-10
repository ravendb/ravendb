import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class continueClusterConfigurationCommand extends commandBase {

    constructor(private dto: Raven.Server.Commercial.ContinueSetupInfo) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.global.setup.setupContinue;

        return this.post<operationIdDto>(url, JSON.stringify(this.dto))
            .fail((response: JQueryXHR) => this.reportError("Failed to configure cluster node", response.responseText, response.statusText));
    }
}

export = continueClusterConfigurationCommand;
