/// <reference path="../../../../typings/tsd.d.ts"/>

import driveUsageDetails = require("models/resources/serverDashboard/driveUsageDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");

class driveUsage {
    
    mountPoint = ko.observable<string>();
    spaceUsed = ko.observable<number>();
    freeSpace = ko.observable<number>();
    freeSpaceLevel = ko.observable<Raven.Server.Dashboard.FreeSpaceLevel>(); 
    
    items = ko.observableArray<driveUsageDetails>([]);

    gridController = ko.observable<virtualGridController<driveUsageDetails>>();
    
    constructor(dto: Raven.Server.Dashboard.MountPointUsage) {
        this.update(dto);
        
        const gridInitialization = this.gridController.subscribe((grid) => {
            grid.headerVisible(true);

            grid.init((s, t) => $.when({
                totalResultCount: this.items().length,
                items: this.items()
            }), () => {
                return [
                    new hyperlinkColumn<driveUsageDetails>(grid, x => x.database(), x => x.database(), "Database", "60%"), //TODO: hyperlink
                    new textColumn<driveUsageDetails>(grid, x => x.size(), "Size", "30%"), //TODO: format
                ];
            });
            
            gridInitialization.dispose();
        });
    }
    
    update(dto: Raven.Server.Dashboard.MountPointUsage) {
        this.mountPoint(dto.MountPoint);
        this.spaceUsed(dto.SpaceUsed);
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
