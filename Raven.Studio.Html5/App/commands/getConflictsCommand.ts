import pagedResultSet = require("common/pagedResultSet");
import commandBase = require("commands/commandBase");
import database = require("models/database");
import conflict = require("models/conflict");
import conflictsInfo = require("models/conflictsInfo");

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

    execute(): JQueryPromise<pagedResultSet> {
        
        var args = {
            sort: "-ConflictDetected",
            start: this.skip,
            pageSize: this.take,
            skipTransformResults: true,
            resultsTransformer: "Raven/ConflictDocumentsTransformer"
        };

        var resultsSelector = (dto: conflictsInfoDto) => new conflictsInfo(dto);
        var url = "/indexes/Raven/ConflictDocuments";
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