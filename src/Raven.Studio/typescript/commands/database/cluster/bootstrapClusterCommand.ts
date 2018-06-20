import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class bootstrapClusterCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.rachisAdmin.adminClusterBootstrap;

        return this.post<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess(`Cluster bootstrap completed. `))
            .fail((response: JQueryXHR) => this.reportError(`Failed to bootstrap cluster`, response.responseText, response.statusText));
    }
}

export = bootstrapClusterCommand;
