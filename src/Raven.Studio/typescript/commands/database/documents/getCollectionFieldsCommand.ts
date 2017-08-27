import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCollectionFieldsCommand extends commandBase {

    constructor(private database: database, private collectionName: string, private prefix: string) {
        super();
    }

    execute(): JQueryPromise<dictionary<string>> {
        const args = {
            collection: this.collectionName,
            prefix: this.prefix
        };

        const url = endpoints.databases.studioCollectionFields.studioCollectionsFields + this.urlEncodeArgs(args);
        return this.query(url, null, this.database);
    }

}

export = getCollectionFieldsCommand;
