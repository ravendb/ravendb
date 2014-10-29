import commandBase = require("commands/commandBase");
import database = require("models/database");
import alert = require("models/alert");

class getOperationAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<alert[]> {
        var url = "/operation/alerts";
        
        return this.query<alert[]>(url, null, this.db, (result:alertDto[]) => result.map(a => new alert(a)));
    }
}

export = getOperationAlertsCommand;