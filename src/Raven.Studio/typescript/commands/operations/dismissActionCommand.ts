import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class dismissActionCommand extends commandBase {

    constructor(private rs: resource, private actionId: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.actionId
        };
        const url = this.rs ? endpoints.databases.databaseNotificationCenter.notificationCenterDismiss : endpoints.global.serverNotificationCenter.notificationCenterDismiss;

        return this.post(url + this.urlEncodeArgs(args), null, this.rs, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to dismiss action", response.responseText, response.statusText));
        
    }
}

export = dismissActionCommand;
