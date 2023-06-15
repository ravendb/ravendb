import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCollectionFieldsCommand extends commandBase {

    private readonly database: database;
    private readonly collectionName: string;
    private readonly prefix: string;

    constructor(database: database, collectionName: string, prefix: string) {
        super();
        this.database = database;
        this.collectionName = collectionName;
        this.prefix = prefix;
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
