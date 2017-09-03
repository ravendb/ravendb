import commandBase = require("commands/commandBase");
import databaseInfo = require("models/resources/info/databaseInfo");
import endpoints = require("endpoints");
import configuration = require("configuration");

class toggleClusterObserverCommand extends commandBase {

    constructor(private suspend: boolean) {
        super();
    }

    execute(): JQueryPromise<void> {
        
        const args = {
            value: this.suspend
        };
        const basicUrl = endpoints.global.rachisAdmin.adminClusterObserverSuspend + this.urlEncodeArgs(args);

        return this.post(basicUrl, null, null, {  dataType: undefined })
            .done(() => {
                const actionPerformed = this.suspend ? "suspended" : "resumed";
                this.reportSuccess("Cluster observer was " + actionPerformed);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to toggle cluster observer state", response.responseText));
    }
}

export = toggleClusterObserverCommand;
