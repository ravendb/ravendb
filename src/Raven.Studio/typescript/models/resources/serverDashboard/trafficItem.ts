/// <reference path="../../../../typings/tsd.d.ts"/>

class trafficItem {
    database = ko.observable<string>();
    requestsPerSecond = ko.observable<number>();
    transferPerSecond = ko.observable<number>();
    
    showOnGraph = ko.observable<boolean>();
    
    constructor(dto: Raven.Server.Dashboard.TrafficWatchItem) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Dashboard.TrafficWatchItem) {
        this.database(dto.Database);
        this.transferPerSecond(dto.TransferPerSecond);
        this.requestsPerSecond(dto.RequestsPerSecond);
    }
}

export = trafficItem;
