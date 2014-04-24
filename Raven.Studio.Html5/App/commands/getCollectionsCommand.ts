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
        
        var args = {
            field: "Tag",
            fromValue: "",
            pageSize: 128
        };

        var resultsSelector = (collectionNames: string[]) => collectionNames.map(n => new collection(n, this.ownerDb));
        return this.query("/terms/Raven/DocumentsByEntityName", args, this.ownerDb, resultsSelector)
            .fail(() => this.reportError("Raven/DocumentsByEntityName index not found",
                        "In order to enjoy the studio interface, please recreate the index: \n" + "index definition: \n" + 'Name: Raven/DocumentsByEntityName \n'+'Map: from doc in docs let Tag = doc["@metadata"]["Raven-Entity-Name"] select new { Tag, LastModified = (DateTime)doc["@metadata"]["Last-Modified"] };'));
    }
}

export = getCollectionsCommand;