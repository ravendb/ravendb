import database = require("models/resources/database");
import collection = require("models/database/documents/collection");

class collectionsStats  {

    numberOfDocuments = ko.observable<number>();
    collections: collection[];
    
    constructor(statsDto: collectionsStatsDto, ownerDatabase: database) {
        this.numberOfDocuments(statsDto.NumberOfDocuments);
        this.collections = statsDto.Collections.map(x => new collection(x.Name, ownerDatabase, x.Count));
    }
   
}

export = collectionsStats;
