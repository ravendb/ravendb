import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class leaderStepDownCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.rachisAdmin.adminClusterReelect;

        return this.post<void>(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to step down current leader", response.responseText));
    }
}

export = leaderStepDownCommand;
