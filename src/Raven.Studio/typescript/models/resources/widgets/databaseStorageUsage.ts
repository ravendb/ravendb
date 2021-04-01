/// <reference path="../../../../typings/tsd.d.ts" />

import databaseDiskUsage = require("models/resources/widgets/databaseDiskUsage");

class databaseStorageUsage {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);
    
    items: databaseDiskUsage[] = [];

    constructor(tag: string) {
        this.tag = tag;
    }
    
    update(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload) {
        this.items = data.Items.map(x => new databaseDiskUsage(nodeTag, x));
    }
}

export = databaseStorageUsage;
