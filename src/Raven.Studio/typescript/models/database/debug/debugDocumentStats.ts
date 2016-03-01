/// <reference path="../../../../typings/tsd.d.ts"/>

class debugDocumentStats {
    total: number;
    totalSize: number;
    tombstones: number;
    system: collectionStats;
    noCollection: collectionStats;
    collections: any[];
    timeToGenerate: string;

    constructor(dto: debugDocumentStatsDto) {
        this.total = dto.Total;
        this.totalSize = dto.TotalSize;
        this.tombstones = dto.Tombstones;
        this.system = dto.System;
        this.noCollection = dto.NoCollection;
        this.collections = $.map(dto.Collections, (data: collectionStats, name: string) => { return { "name": name, "stats": data.Stats, "size": data.TotalSize, "topDocs": data.TopDocs }; });
        this.timeToGenerate = dto.TimeToGenerate;
    }

}

export = debugDocumentStats;
