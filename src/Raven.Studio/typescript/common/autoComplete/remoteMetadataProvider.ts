import database = require("models/resources/database");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");

class remoteMetadataProvider implements queryCompleterProviders {
    
    private readonly database: KnockoutObservable<database>;
    private readonly indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>;
    
    constructor(activeDatabase: KnockoutObservable<database>, indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>) {
        this.database = activeDatabase;
        this.indexes = indexes;
    }

    collections(callback: (collectionNames: string[]) => void): void {
        callback(collectionsTracker.default.getCollectionNames());
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        new getIndexEntriesFieldsCommand(indexName, this.database(), false)
            .execute()
            .done(result => {
                callback(result.Static);
            });
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        if (collectionName === "@all_docs") {
            collectionName = "All Documents";
        }
        const matchedCollection = collectionsTracker.default.collections().find(x => x.name === collectionName);
        if (matchedCollection) {
            matchedCollection.fetchFields(prefix)
                .done(result => {
                    if (result) {
                        if (!prefix) {
                            result["id()"] = "String";
                        }
                        callback(result);
                    }
                });
        } else {
            callback({});
        }
    }

    indexNames(callback: (indexNames: string[]) => void): void {
        callback(this.indexes().map(x => x.Name));
    }
}


export = remoteMetadataProvider;
