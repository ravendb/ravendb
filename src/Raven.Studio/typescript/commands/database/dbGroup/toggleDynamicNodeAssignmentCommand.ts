import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class toggleDynamicNodeAssignmentCommand extends commandBase {

    constructor(private databaseName: string, private enable: boolean) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.databaseName,
            enable: this.enable
        };
        const url = endpoints.global.adminDatabases.adminDatabasesDynamicNodeDistribution + this.urlEncodeArgs(args);

        return this.post<void>(url, null, null,  { dataType: undefined })
            .done(() => this.reportSuccess("Dynamic node distribution was turned " + (this.enable ? "on" : "off")))
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle node distribution mode", response.responseText, response.statusText));
    }
}

export = toggleDynamicNodeAssignmentCommand;
