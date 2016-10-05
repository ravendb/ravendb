import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import collectionsStats = require("models/database/documents/collectionsStats");
import collection = require("models/database/documents/collection");
import endpoints = require("endpoints");

class getCollectionsStatsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<collectionsStats> {
        var finalResult = $.Deferred<collectionsStats>();
        this.query<collectionsStatsDto>(endpoints.databases.collections.collectionsStats, null, this.ownerDb)
            .done(results => {
                var stats = new collectionsStats(results, this.ownerDb);
                finalResult.resolve(stats);
            })
            .fail((response) => {
                this.reportError("Can't fetch collection stats");
                finalResult.reject(response.responseText);
            });

        return finalResult;
    }
}

export = getCollectionsStatsCommand;
