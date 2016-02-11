import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class deleteCollectionCommand extends commandBase {

    private displayCollectionName: string;

    constructor(private collectionName: string, private db: database) {
        super();

        this.displayCollectionName = (collectionName === "*") ? "All Documents" : collectionName;
    }

    execute(): JQueryPromise<void> {
        this.reportInfo("Deleting " + this.displayCollectionName);

        var url = "/collections/docs";
        var args = {
            name: this.collectionName
        };
        return this.del(url, args, this.db);
    }
}

export = deleteCollectionCommand; 
