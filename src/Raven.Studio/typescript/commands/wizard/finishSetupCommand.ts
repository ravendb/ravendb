import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class finishSetupCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.setup.setupFinish;

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to save configuration", response.responseText, response.statusText));
    }
}

export = finishSetupCommand;
