import commandBase = require("commands/commandBase");
import database = require("models/database");
import collection = require("models/collection");

class getCollectionsCommand extends commandBase {

    /**
	* @param ownerDb The database the collections will belong to.
	*/
    constructor(private ownerDb: database) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<collection[]> {
       

        var finalResult = $.Deferred<collection[]>();
        this.runQuery()
            .fail((xhr: JQueryXHR) => this.createSystemIndexAndTryAgain(finalResult, xhr))
            .done((results: collection[]) => finalResult.resolve(results));
        return finalResult;
    }

    runQuery(): JQueryPromise<collection[]> {
        var args = {
            field: "Tag",
            fromValue: "",
            pageSize: 128
        };
        var resultsSelector = (collectionNames: string[]) => collectionNames.map(n => new collection(n, this.ownerDb));
        return this.query("/terms/Raven/DocumentsByEntityName", args, this.ownerDb, resultsSelector)
    }

    createSystemIndexAndTryAgain(deferred: JQueryDeferred<collection[]>, originalReadError: JQueryXHR) {
        // Most often, failure to get the collections is due to the missing system index, Raven/DocumentsByEntityName.
        // This appears to be new behavior as of 3.0: Raven doesn't create this index automatically for the system database.

        // Calling silverlight/ensureStartup creates the system index.
        this.query("/silverlight/ensureStartup", null, this.ownerDb)
            .done(() => this.retryQuery(deferred, originalReadError))
            .fail(() => this.onErrorReadingCollections(deferred, originalReadError));
    }

    retryQuery(deferred: JQueryDeferred<collection[]>, originalReadError: JQueryXHR) {
        this.runQuery()
            .fail(() => this.onErrorReadingCollections(deferred, originalReadError))
            .done((results: collection[]) => deferred.resolve(results));
    }

    onErrorReadingCollections(deferred: JQueryDeferred<collection[]>, xhr: JQueryXHR) {
        this.reportError("Failed to read collections", xhr.responseText, xhr.statusText);
        deferred.reject(xhr);
    }
}

export = getCollectionsCommand;