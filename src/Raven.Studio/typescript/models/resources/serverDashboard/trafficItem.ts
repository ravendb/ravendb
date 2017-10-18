/// <reference path="../../../../typings/tsd.d.ts"/>

class trafficItem {
    database = ko.observable<string>();
    requestsPerSecond = ko.observable<number>();
    writesPerSecond = ko.observable<number>();
    dataWritesPerSecond = ko.observable<number>();
    
    showOnGraph = ko.observable<boolean>();
    
    constructor(dto: Raven.Server.Dashboard.TrafficWatchItem) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Dashboard.TrafficWatchItem) {
        this.database(dto.Database);
        this.requestsPerSecond(dto.RequestsPerSecond);
        this.writesPerSecond(dto.WritesPerSecond);
        this.dataWritesPerSecond(dto.WriteBytesPerSecond);
    }
}

export = trafficItem;
