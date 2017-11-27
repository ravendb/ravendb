import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class postponeNotificationCommand extends commandBase {

    constructor(private db: database, private notificationId: string, private timeInSec: number) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.notificationId,
            timeInSec: this.timeInSec
        };
        const url = this.db ? endpoints.databases.databaseNotificationCenter.notificationCenterPostpone : endpoints.global.serverNotificationCenter.serverNotificationCenterPostpone;

        return this.post(url + this.urlEncodeArgs(args), null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to postpone action", response.responseText, response.statusText));
        
    }
}

export = postponeNotificationCommand;
