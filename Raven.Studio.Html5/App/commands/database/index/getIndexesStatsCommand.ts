import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexesStatsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<indexDataDto[]> {
        var url = "/indexes-stats";
        return this.query(url, null, this.db);
    }
}

export = getIndexesStatsCommand;
