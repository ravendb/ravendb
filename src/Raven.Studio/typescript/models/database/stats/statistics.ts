/// <reference path="../../../../typings/tsd.d.ts"/>
import indexStatistics = require("models/database/stats/indexStatistics");
import changeVectorUtils = require("common/changeVectorUtils");
import generalUtils = require("common/generalUtils");

class statistics {
    databaseId: string;
    databaseChangeVector: changeVectorItem[];
    lastDocEtag: string;
    countOfIndexes: string;
    countOfDocuments: string;
    countOfAttachments: string;
    countOfUniqueAttachments: string;
    countOfDocumentsConflicts: string;
    countOfTombstones: string;
    is64Bit: boolean;
    indexPerformanceURL: string;
    dataSizeOnDisk: string;
    tempBuffersSizeOnDisk: string;
    totalSizeOnDisk: string;
    
    // The observable indexes array, ordered by type
    indexesByType = ko.observableArray<indexesWithType>(); 
    
    constructor(dbStats: Raven.Client.Documents.Operations.DatabaseStatistics, indexStats: Raven.Client.Documents.Indexes.IndexStats[]) {
        this.databaseId = dbStats.DatabaseId;

        this.databaseChangeVector = changeVectorUtils.formatChangeVector(dbStats.DatabaseChangeVector, changeVectorUtils.shouldUseLongFormat([dbStats.DatabaseChangeVector]));
        this.lastDocEtag = dbStats.LastDocEtag.toLocaleString();;
        this.countOfDocuments = dbStats.CountOfDocuments.toLocaleString();
        this.countOfIndexes = dbStats.CountOfIndexes.toLocaleString();
        this.countOfAttachments = dbStats.CountOfAttachments.toLocaleString();
        this.countOfUniqueAttachments = dbStats.CountOfUniqueAttachments.toLocaleString();
        this.countOfDocumentsConflicts = dbStats.CountOfDocumentsConflicts.toLocaleString();
        this.countOfTombstones = dbStats.CountOfTombstones.toLocaleString();
        this.is64Bit = dbStats.Is64Bit;
        this.dataSizeOnDisk = generalUtils.formatBytesToSize(dbStats.SizeOnDisk.SizeInBytes);
        this.tempBuffersSizeOnDisk = generalUtils.formatBytesToSize(dbStats.TempBuffersSizeOnDisk.SizeInBytes);
        this.totalSizeOnDisk = generalUtils.formatBytesToSize(dbStats.SizeOnDisk.SizeInBytes + dbStats.TempBuffersSizeOnDisk.SizeInBytes);
        
        
        // 1. Create the array with all indexes that we got from the endpoint
        const allIndexes = indexStats.map(x => new indexStatistics(x));

        // 2. Create an array where indexes are ordered by type
        let indexesByTypeTemp = Array<indexesWithType>();
        allIndexes.forEach(index => {
            let existingEntry = indexesByTypeTemp.find(x => x.indexType === index.indexType);
            if (!existingEntry) {
                // A new type encountered
                const newType = new indexesWithType(index.indexType);
                newType.add(index);
                indexesByTypeTemp.push(newType);
            }
            else {
                // Type already exists, only add the index
                existingEntry.add(index);
            }
        });

        // 3. Sort by index name & type
        indexesByTypeTemp.forEach(x => { x.indexes.sort((a, b) => a.indexName > b.indexName ? 1 : -1); });
        indexesByTypeTemp.sort((a, b) => a.indexType > b.indexType ? 1 : -1);

        // 4. Update the observable array 
        this.indexesByType(indexesByTypeTemp);
    }
}

class indexesWithType {
    indexType: string;
    indexes: indexStatistics[];

    constructor(type: string) {
        this.indexType = type;
        this.indexes = [];
    }

    add(index: indexStatistics) {
        this.indexes.push(index);
    }
}

export = statistics;
