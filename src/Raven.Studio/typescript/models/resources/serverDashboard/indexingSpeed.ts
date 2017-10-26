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
        this.indexedPerSecond(this.round(dto.IndexedPerSecond));
        this.mappedPerSecond(this.round(dto.MappedPerSecond));
        this.reducedPerSecond(this.round(dto.ReducedPerSecond));
    }

    private round(num: number): number {
        if (num < 1) {
            return num;
        }

        return Math.round(num);
    }
}

export = indexingSpeed;
