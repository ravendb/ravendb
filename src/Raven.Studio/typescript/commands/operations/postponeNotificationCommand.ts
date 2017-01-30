import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class postponeNotificationCommand extends commandBase {

    constructor(private rs: resource, private notificationId: string, private timeInSec: number) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.notificationId,
            timeInSec: this.timeInSec
        };
        const url = this.rs ? endpoints.databases.databaseNotificationCenter.notificationCenterPostpone : endpoints.global.serverNotificationCenter.notificationCenterPostpone;

        return this.post(url + this.urlEncodeArgs(args), null, this.rs, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to postpone action", response.responseText, response.statusText));
        
    }
}

export = postponeNotificationCommand;
