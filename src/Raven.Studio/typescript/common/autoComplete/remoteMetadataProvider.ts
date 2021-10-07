import database = require("models/resources/database");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");

class remoteMetadataProvider implements queryCompleterProviders {
    
    private readonly database: KnockoutObservable<database>;
    private readonly indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>;
    private readonly queryType: rqlQueryType; //TODO: do we need that?
    
    constructor(activeDatabase: KnockoutObservable<database>, indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>, queryType: rqlQueryType) {
        this.database = activeDatabase;
        this.indexes = indexes;
        this.queryType = queryType;
    }
    
    terms(indexName: string, collection: string, field: string, pageSize: number, callback: (terms: string[]) => void): void {
        new getIndexTermsCommand(indexName, collection, field, this.database(), pageSize)
            .execute()
            .done(terms => {
                callback(terms.Terms);
            });
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
        if (collectionName === "@all_docs"){
            collectionName = "All Documents";
        }
        const matchedCollection = collectionsTracker.default.collections().find(x => x.name === collectionName);
        if (matchedCollection) {
            matchedCollection.fetchFields(prefix)
                .done(result => {
                    if (result) {
                        callback(result);
                    }
                });
        }
    }

    indexNames(callback: (indexNames: string[]) => void): void {
        callback(this.indexes().map(x => x.Name));
    }
}


export = remoteMetadataProvider;
