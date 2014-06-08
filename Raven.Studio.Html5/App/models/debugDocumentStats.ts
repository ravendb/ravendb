import customColumnParams = require("models/customColumnParams");
import document = require("models/document");

class debugDocumentStats {
    total: number;
    tombstones: number;
    system: number;
    noCollection: number;
    collections: any[];
    timeToGenerate: string;

    constructor(dto: debugDocumentStatsDto) {
        this.total = dto.Total;
        this.tombstones = dto.Tombstones;
        this.system = dto.System;
        this.noCollection = dto.NoCollection;
        this.collections = $.map(dto.Collections, (data: collectionStats, name: string) => { return { "name": name, "count": data.Quantity, "size": data.Size }; });
        this.timeToGenerate = dto.TimeToGenerate;
    }

}

export = debugDocumentStats;