import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");

class getOperationAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<alert[]> {
        var url = "/operation/alerts";//TODO: use endpoints
        
        return this.query<alert[]>(url, null, this.db, (result: alertDto[]) => result.map(a => new alert(a)));
    }
}

export = getOperationAlertsCommand;
