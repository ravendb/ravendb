import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getCollectionsCountCommand = require("commands/database/documents/getCollectionsCountCommand");
import getCachedCollectionsCount = require("commands/database/studio/getCachedCollectionsCount");

class getCollectionsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database, private previousValues: collection[]=[], private lastQueryDate: KnockoutObservable<string> = null) {
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
        var task = $.Deferred();

        new getIndexTermsCommand("Raven/DocumentsByEntityName", "Tag", this.ownerDb)
            .execute()
            .done((terms: string[], status, xhr) => {

                if (this.previousValues.length > 0 && this.lastQueryDate()) {

                    var collections = this.previousValues.filter(v => !v.isAllDocuments && !v.isSystemDocuments).map(v => new collection(v.name, this.ownerDb, v.documentCount()));
                    // apply optimalization fetch only changes, based on last query time. 
                    new getCachedCollectionsCount(this.ownerDb, this.lastQueryDate())
                        .execute()
                        .done((counts: Array<{ CollectionName: string; Count: number}>) => {
                            counts.forEach(countData => {
                                var c = collections.first(p => p.name == countData.CollectionName);
                                if (c) {
                                    c.documentCount(countData.Count);
                                } else {
                                    collections.push(new collection(countData.CollectionName, this.ownerDb, countData.Count));
                                }
                            });
                            task.resolve(collections);
                        })
                        .fail(result => task.reject(result));
                } else {
                    var collections = terms.map(term => new collection(term, this.ownerDb, 0));
                    new getCollectionsCountCommand(collections, this.ownerDb)
                        .execute()
                        .done(result => task.resolve(result))
                        .fail(result => task.reject(result));
                }
                if (this.lastQueryDate != null) {
                    this.lastQueryDate(xhr.getResponseHeader('Date'));
                }
            })
            .fail((response) => {
                this.createSystemIndexAndTryAgain(task, response);
            });

        return task;
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
