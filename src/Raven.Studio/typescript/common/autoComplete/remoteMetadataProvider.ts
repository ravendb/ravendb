import database = require("models/resources/database");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import getCollectionFieldsCommand from "commands/database/documents/getCollectionFieldsCommand";
import IndexUtils from "components/utils/IndexUtils";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";

class remoteMetadataProvider implements queryCompleterProviders {
    
    private readonly db: database | DatabaseSharedInfo;
    private readonly indexes: () => string[];
    
    constructor(database: database | DatabaseSharedInfo, indexes: () => string[]) {
        this.db = database;
        this.indexes = indexes;
    }

    collections(callback: (collectionNames: string[]) => void): void {
        callback(collectionsTracker.default.getCollectionNames());
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        const locations = this.db instanceof database ? this.db.getLocations() : DatabaseUtils.getLocations(this.db);

        new getIndexEntriesFieldsCommand(indexName, this.db.name, locations[0], false)
            .execute()
            .done(result => {
                callback(result.Static.filter(x => !IndexUtils.FieldsToHideOnUi.includes(x)));
            });
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        if (collectionName === "@all_docs") {
            collectionName = "All Documents";
        }
        const matchedCollection = collectionsTracker.default.collections().find(x => x.name === collectionName);
        if (matchedCollection) {
            const collectionName = matchedCollection.isAllDocuments ? undefined : matchedCollection.name;
            new getCollectionFieldsCommand(this.db.name, collectionName, prefix)
                .execute()
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
        callback(this.indexes());
    }
}


export = remoteMetadataProvider;
