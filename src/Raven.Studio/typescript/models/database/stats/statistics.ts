/// <reference path="../../../../typings/tsd.d.ts"/>

import indexStatistics = require("models/database/stats/indexStatistics");

class statistics {

    storageEngine: string;
    dataBaseId: string;
    lastDocEtag?: number;
    lastAttachmentEtag: number;
    staleIndexes: string[];
    countOfIndexes: number;
    countOfDocuments: number;
    countOfTransformers: number;
    //lastIndexingDateTime: number; // ??
    is64Bit: boolean;

    indexes = ko.observableArray<indexStatistics>();

    constructor(dto: Raven.Client.Data.DatabaseStatistics) {
        this.storageEngine = "Voron";
        this.dataBaseId = dto.DatabaseId; 
        this.lastDocEtag = dto.LastDocEtag;
        this.countOfIndexes = dto.CountOfIndexes;
        this.countOfDocuments = dto.CountOfDocuments;
        this.staleIndexes = dto.StaleIndexes;
        //this.lastIndexingDateTime = dto.?? need to do in different issue
        this.countOfTransformers = dto.CountOfTransformers;
        this.is64Bit = dto.Is64Bit;

        // TODO: work on other props

        this.indexes(dto.Indexes.map(x => new indexStatistics(x)));
    }

}

export = statistics;

//interface DatabaseStatistics {
//    ApproximateTaskCount: number;
//    CountOfDocuments: number;
//    CountOfIndexes: number;
//    CountOfRevisionDocuments?: number;
//    CountOfTransformers: number;
//    CurrentNumberOfItemsToIndexInSingleBatch: number;
//    CurrentNumberOfItemsToReduceInSingleBatch: number;
//    CurrentNumberOfParallelTasks: number;
//    DatabaseId: string;
//    Indexes: Raven.Client.Data.IndexInformation[];
//    Is64Bit: boolean;
//    LastDocEtag?: number;
//    StaleIndexes: string[];
//}
