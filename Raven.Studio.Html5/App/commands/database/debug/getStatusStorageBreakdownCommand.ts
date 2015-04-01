import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusStorageBreakdownCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/admin/detailed-storage-breakdown";
        return this.query<any>(url, null, this.db);
    }
}

export = getStatusStorageBreakdownCommand;