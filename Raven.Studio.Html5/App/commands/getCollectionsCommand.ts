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

        return this.runQuery()
            .fail((xhr: JQueryXHR) => this.createSystemIndexAndTryAgain(finalResult, xhr))
            .done((results: collection[]) => finalResult.resolve(results));
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
        // This appears to be new behavior as of 3.0: Raven doesn't create this index automatically.

        // So, if we are unable to read the collections, we create the system index ourselves, then try again.
        require(["commands/saveIndexDefinitionCommand"], (saveIndexDefinitionCommandCtor: any) => {
            var systemIndex = this.createSystemIndex();

            // Ctor in this case is the constructor for saveIndexDefinitionCommand.
            new saveIndexDefinitionCommandCtor(systemIndex, 0, this.ownerDb)
                .execute()
                .fail(() => this.onErrorReadingCollections(deferred, originalReadError))
                .done(() => this.retryQuery(deferred, originalReadError));
        });
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

    createSystemIndex(): indexDefinitionDto {
        var map = 'from doc in docs let Tag = doc["@metadata"]["Raven-Entity-Name"] select new { Tag, LastModified = (DateTime)doc["@metadata"]["Last-Modified"] }';
        return {
            Analyzers: {},
            Fields: [],
            Indexes: {},
            InternalFieldsMapping: {},
            IsCompiled: false,
            IsMapReduce: false,
            LockMode: "Unlock",
            Map: map,
            Maps: [map],
            Name: "Raven/DocumentsByEntityName",
            Reduce: null,
            SortOptions: {},
            SpatialIndexes: {},
            Stores: {},
            Suggestions: {},
            TermVectors: {},
            TransformResults: null,
            Type: "Map",
            MaxIndexOutputsPerDocument: null
        };
    }
}

export = getCollectionsCommand;