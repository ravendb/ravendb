import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getStatusDebugQueriesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<statusDebugQueriesGroupDto[]> {
        var url = "/debug/queries";//TODO: use endpoints
        var resultSelector =
            (result: any) => $.map(result, (queries, key) => { return { IndexName: key, Queries: <Array<statusDebugQueriesQueryDto>>queries } });
        return this.query<Array<statusDebugQueriesGroupDto>>(url, null, this.db, resultSelector);
    }
}

export = getStatusDebugQueriesCommand;
