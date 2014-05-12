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
        this.collections = $.map(dto.Collections, (v, k) => { return {"name": k, "count": v } });
        this.timeToGenerate = dto.TimeToGenerate;
    }

}

export = debugDocumentStats;