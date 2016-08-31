import pagedResultSet = require("common/pagedResultSet");
import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import conflictsInfo = require("models/database/replication/conflictsInfo");

class getConflictsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database, private skip: number, private take: number) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<pagedResultSet<any>> {
        
        var args = {
            sort: "-ConflictDetectedAt",
            start: this.skip,
            pageSize: this.take,
            resultsTransformer: "Raven/ConflictDocumentsTransformer"
        };

        var resultsSelector = (dto: conflictsInfoDto) => new conflictsInfo(dto);
        var url = "/indexes/Raven/ConflictDocuments";//TODO: use endpoints
        var conflictsTask = $.Deferred();
        this.query<conflictsInfo>(url, args, this.ownerDb, resultsSelector).
            fail(response => conflictsTask.reject(response)).
            done(conflicts => {
                var items = conflicts.results;
                var resultsSet = new pagedResultSet(items, conflicts.totalResults);
                conflictsTask.resolve(resultsSet);
            });

        return conflictsTask;
    }
}

export = getConflictsCommand;
