import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class postponeActionCommand extends commandBase {

    constructor(private rs: resource, private actionId: string, private timeInSec: number) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.actionId,
            timeInSec: this.timeInSec
        };
        const url = this.rs ? endpoints.databases.databaseNotificationCenter.notificationCenterPostpone : endpoints.global.serverNotificationCenter.notificationCenterPostpone;

        return this.post(url + this.urlEncodeArgs(args), null, this.rs, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to postpone action", response.responseText, response.statusText));
        
    }
}

export = postponeActionCommand;
