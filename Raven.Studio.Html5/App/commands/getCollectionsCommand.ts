import commandBase = require("commands/commandBase");
import database = require("models/database");
import collection = require("models/collection");

class createDatabaseCommand extends commandBase {

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
        return this.query("/terms/Raven/DocumentsByEntityName", args, this.ownerDb, resultsSelector);
    }
}

export = createDatabaseCommand;