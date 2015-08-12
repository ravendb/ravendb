import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class enableQueryTimings extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        var url = "/debug/enable-query-timing";
        return this.query(url, null, this.db);
    }
}

export = enableQueryTimings;