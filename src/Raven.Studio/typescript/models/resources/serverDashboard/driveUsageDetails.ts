/// <reference path="../../../../typings/tsd.d.ts"/>

class driveUsageDetails {
    
    database = ko.observable<string>();
    size = ko.observable<number>();
    
    constructor(dto: Raven.Server.Dashboard.DatabaseDiskUsage) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Dashboard.DatabaseDiskUsage) {
        this.database(dto.Database);
        this.size(dto.Size);
    }
}

export = driveUsageDetails;
