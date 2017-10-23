/// <reference path="../../../../typings/tsd.d.ts"/>

class indexingSpeed {
    database = ko.observable<string>();
    
    indexedPerSecond = ko.observable<number>();
    mappedPerSecond = ko.observable<number>();
    reducedPerSecond = ko.observable<number>();
    
    showOnGraph = ko.observable<boolean>();
    
    constructor(dto: Raven.Server.Dashboard.IndexingSpeedItem) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Dashboard.IndexingSpeedItem) {
        this.database(dto.Database);
        this.indexedPerSecond(dto.IndexedPerSecond);
        this.mappedPerSecond(dto.MappedPerSecond);
        this.reducedPerSecond(dto.ReducedPerSecond);
    }
}

export = indexingSpeed;
