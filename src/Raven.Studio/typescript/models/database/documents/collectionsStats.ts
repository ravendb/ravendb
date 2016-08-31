import database = require("models/resources/database");
import collection = require("models/database/documents/collection");

class collectionsStats  {

    numberOfDocuments = ko.observable<number>();
    collections: collection[];
    
    constructor(statsDto: collectionsStatsDto, ownerDatabase: database) {
        this.numberOfDocuments(statsDto.NumberOfDocuments);
        this.collections = [];

        for (var key in statsDto.Collections) {
            if (!statsDto.Collections.hasOwnProperty(key))
                continue;
            this.collections.push(new collection(key, ownerDatabase, statsDto.Collections[key]));
        }
    }

    getCollectionCount(collectionName: string): number {
        const matchedCollection = this.collections.first(x => x.name === collectionName);
        return matchedCollection ? matchedCollection.documentCount() : 0;
    }
   
}

export = collectionsStats;
