import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");

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
        // TODO: Someone that actually knows TypeScript need to look at this and see how bad I messed up
        this.query("/databases/" + this.ownerDb.name + "/collections/stats", null, null)
            .done( results => {
                var collections = results.Collections.map(result => new collection(result.Name, this.ownerDb, result.Count));
                finalResult.done(collections);
            })
            .fail((response) => {
                this.reportError("Can't fetch collection names");
                finalResult.reject(response.responseText);
            });

        return finalResult;
    }
}

export = getCollectionsCommand;
