import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import queryUtil = require("common/queryUtil");

class getCollectionsCountCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private collections: collection[], private ownerDb: database) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<collection[]> {
        var task = $.Deferred();

        var requests = this.collections.map(collection => {
            return {
                Url: "/indexes/Raven/DocumentsByEntityName",
                Headers: {},
                Query: "?&query=Tag:" + queryUtil.escapeTerm(collection.name) + "&pageSize=0"
            }
        });

        this.post("/multi_get?parallel=yes", JSON.stringify(requests), this.ownerDb, null, 0)
            .done((result) => {
                for (var i = 0; i < this.collections.length; i++) {
                    this.collections[i].documentCount(result[i].Result.TotalResults || 0);
                }
                task.resolve(this.collections);
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to fetch collections count", response.responseText, response.statusText);
                    task.reject(response);
                });

        return task;
    }
}

export = getCollectionsCountCommand;
