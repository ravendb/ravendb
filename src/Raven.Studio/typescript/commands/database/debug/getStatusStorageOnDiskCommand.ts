import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusStorageOnDiskCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = "/database/storage/sizes";//TODO: use endpoints
        return this.query<any>(url, null, this.db);
    }
}

export = getStatusStorageOnDiskCommand;
