import commandBase = require("commands/commandBase");
import database = require("models/database");

class getOperationAlertsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var url = "/operation/alerts";
        return this.query<string[]>(url, null, this.db);
    }
}

export = getOperationAlertsCommand;