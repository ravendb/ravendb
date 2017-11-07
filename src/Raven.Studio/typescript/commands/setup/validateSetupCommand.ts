import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class validateSetupCommand extends commandBase {

    constructor(private setupMode: Raven.Server.Commercial.SetupMode, private operationId: number, private dto: Raven.Server.Commercial.SetupInfo) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            setupMode: this.setupMode,
            operationId: this.operationId
        };
        const url = endpoints.global.setup.setupValidate + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to validate configuration", response.responseText, response.statusText));
    }
}

export = validateSetupCommand;
