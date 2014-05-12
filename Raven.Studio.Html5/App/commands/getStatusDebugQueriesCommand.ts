import commandBase = require("commands/commandBase");
import database = require("models/database");
import statusDebugQueriesGroup = require("models/statusDebugQueriesGroup");

class getStatusDebugQueriesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<statusDebugQueriesGroup[]> {
        var url = "/debug/queries";
        var resultSelector = (result) => $.map(result, (queries, key) => new statusDebugQueriesGroup({ "IndexName" : key, "Queries": queries }) );
        return this.query<Array<statusDebugQueriesGroup>>(url, null, this.db, resultSelector);
    }
}

export = getStatusDebugQueriesCommand;