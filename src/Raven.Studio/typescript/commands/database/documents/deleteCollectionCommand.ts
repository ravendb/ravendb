import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteCollectionCommand extends commandBase {
    private displayCollectionName: string;

    constructor(private collectionName: string, private db: database) {
        super();

        this.displayCollectionName = (collectionName === "*") ? "All Documents" : collectionName;
    }

    execute(): JQueryPromise<operationIdDto> {
        this.reportInfo("Deleting " + this.displayCollectionName);
        var args = {
            name: this.collectionName
        };
        var url = endpoints.databases.collections.collectionsDocs + this.urlEncodeArgs(args);

        return this.del(url, null, this.db, { dataType: undefined });
    }
}

export = deleteCollectionCommand; 
