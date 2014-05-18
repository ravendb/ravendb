import commandBase = require("commands/commandBase");
import database = require("models/database");

class getIndexingStatusCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        var url = '/admin/indexingStatus';
        var result = this.query(url, null, this.db);
        return result;
    }

}

export = getIndexingStatusCommand;