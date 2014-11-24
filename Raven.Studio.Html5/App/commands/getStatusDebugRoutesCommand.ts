import commandBase = require("commands/commandBase");
import database = require("models/database");
import appUrl = require("common/appUrl");

class getStatusDebugConfigCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/debug/routes";
        var db = appUrl.getSystemDatabase();
        return this.query<any>(url, null, db);
    }
}

export = getStatusDebugConfigCommand;