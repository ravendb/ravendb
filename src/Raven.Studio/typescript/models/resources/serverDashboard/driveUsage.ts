/// <reference path="../../../../typings/tsd.d.ts"/>

import driveUsageDetails = require("models/resources/serverDashboard/driveUsageDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");

class driveUsage {
    private sizeFormatter = generalUtils.formatBytesToSize;
    
    mountPoint = ko.observable<string>();
    totalCapacity = ko.observable<number>(0);
    freeSpace = ko.observable<number>(0);
    freeSpaceLevel = ko.observable<Raven.Server.Dashboard.FreeSpaceLevel>();
    
    freeSpaceLevelClass: KnockoutComputed<string>;
    percentageUsage: KnockoutComputed<number>;
    
    items = ko.observableArray<driveUsageDetails>([]);
    totalDocumentsSpaceUsed: KnockoutComputed<number>;

    gridController = ko.observable<virtualGridController<driveUsageDetails>>();
    
    constructor(dto: Raven.Server.Dashboard.MountPointUsage) {
        this.update(dto);
        
        this.freeSpaceLevelClass = ko.pureComputed(() => {
            const level = this.freeSpaceLevel();
            switch (level) {
                case "High":
                    return "text-success";
                case "Medium":
                    return "text-warning";
                case "Low":
                    return "text-danger";
            }
            return "";
        });

        this.percentageUsage = ko.pureComputed(() => {
            const total = this.totalCapacity();
            const free = this.freeSpace();
            if (!total) {
                return 0;
            }
            return (total - free) * 100 / total;
        });
        
        this.totalDocumentsSpaceUsed = ko.pureComputed(() => {
           return _.sum(this.items().map(x => x.size()));
        });
        
        const gridInitialization = this.gridController.subscribe((grid) => {
            grid.headerVisible(true);

            grid.init((s, t) => $.when({
                totalResultCount: this.items().length,
                items: this.items()
            }), () => {
                return [
                    new hyperlinkColumn<driveUsageDetails>(grid, x => x.database(), x => appUrl.forDocuments(null, x.database()), "Database", "60%"),
                    new textColumn<driveUsageDetails>(grid, x => this.sizeFormatter(x.size()), "Size", "30%"),
                ];
            });
            
            gridInitialization.dispose();
        });
    }
    
    update(dto: Raven.Server.Dashboard.MountPointUsage) {
        this.mountPoint(dto.MountPoint);
        this.totalCapacity(dto.TotalCapacity);
        this.freeSpace(dto.FreeSpace);
        this.freeSpaceLevel(dto.FreeSpaceLevel);
        
        const newDbs = dto.Items.map(x => x.Database);
        const oldDbs = this.items().map(x => x.database());
        
        const removed = _.without(oldDbs, ...newDbs);
        removed.forEach(dbName => {
            const matched = this.items().find(x => x.database() === dbName);
            this.items.remove(matched);
        });
        
        dto.Items.forEach(incomingItem => {
            const matched = this.items().find(x => x.database() === incomingItem.Database);
            if (matched) {
                matched.update(incomingItem);
            } else {
                this.items.push(new driveUsageDetails(incomingItem));
            }
        });
        
        if (this.gridController()) {
            this.gridController().reset(false);
        }
    }
}

export = driveUsage;
