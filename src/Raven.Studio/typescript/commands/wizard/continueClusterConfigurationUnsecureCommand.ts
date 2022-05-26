import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class continueClusterConfigurationUnsecureCommand extends commandBase {

    constructor(private operationId: number, private dto: Raven.Server.Commercial.ContinueSetupInfo) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            operationId: this.operationId
        }
        const url = endpoints.global.setup.setupContinueUnsecured + this.urlEncodeArgs(args);

        return this.post<operationIdDto>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to configure cluster node in Unsecure mode", response.responseText, response.statusText));
    }
}

export = continueClusterConfigurationUnsecureCommand;
