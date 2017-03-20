import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import conflictsInfo = require("models/database/replication/conflictsInfo");
import conflict = require("models/database/replication/conflict");

class getConflictsCommand extends commandBase {

    constructor(private ownerDb: database, private skip: number, private take: number) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<pagedResult<any>> {
        
        var args = {
            sort: "-ConflictDetectedAt",
            start: this.skip,
            pageSize: this.take,
            resultsTransformer: "Raven/ConflictDocumentsTransformer"
        };

        var resultsSelector = (dto: conflictsInfoDto) => new conflictsInfo(dto);
        var url = "/indexes/Raven/ConflictDocuments";//TODO: use endpoints
        var conflictsTask = $.Deferred<pagedResult<conflict>>();
        this.query<conflictsInfo>(url, args, this.ownerDb, resultsSelector).
            fail(response => conflictsTask.reject(response)).
            done(conflicts => {
                var items = conflicts.results;
                conflictsTask.resolve({
                    items: items,
                    totalResultCount: conflicts.totalResults
                });
            });

        return conflictsTask;
    }
}

export = getConflictsCommand;
