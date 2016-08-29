import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class dismissAlertCommand extends commandBase {

    constructor(private db: database, private alertUniqueKey: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/operation/alert/dismiss";//TODO: use endpoints
        var args = {
            key: this.alertUniqueKey
        }
        return this.post(url, JSON.stringify(args), this.db, { dataType: 'text' })
            .fail((response: JQueryXHR) => this.reportError("Failed to dismiss alert", response.responseText, response.statusText));
        
    }
}

export = dismissAlertCommand;
