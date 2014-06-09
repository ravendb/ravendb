import customColumnParams = require("models/customColumnParams");
import document = require("models/document");

class debugDocumentStats {
    total: number;
    totalSize: number;
    tombstones: number;
    system: number;
    systemSize: number;
    noCollection: number;
    noCollectionSize: number;
    collections: any[];
    timeToGenerate: string;

    constructor(dto: debugDocumentStatsDto) {
        this.total = dto.Total;
        this.totalSize = dto.TotalSize;
        this.tombstones = dto.Tombstones;
        this.system = dto.System;
        this.systemSize = dto.SystemSize;
        this.noCollection = dto.NoCollection;
        this.noCollectionSize = dto.NoCollectionSize;
        this.collections = $.map(dto.Collections, (data: collectionStats, name: string) => { return { "name": name, "count": data.Quantity, "size": data.Size }; });
        this.timeToGenerate = dto.TimeToGenerate;
    }

}

export = debugDocumentStats;