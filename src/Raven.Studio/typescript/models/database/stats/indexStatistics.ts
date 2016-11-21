/// <reference path="../../../../typings/tsd.d.ts"/>

class indexStatistics {
    indexId: number;
    isStale: boolean;
    name: string;
    type: Raven.Client.Data.Indexes.IndexType;

    constructor(dto: Raven.Client.Data.IndexInformation) {
        this.name = dto.Name;
        this.type = dto.Type;
        this.indexId  = dto.IndexId;
        this.isStale = dto.IsStale;
    }
}

export = indexStatistics;