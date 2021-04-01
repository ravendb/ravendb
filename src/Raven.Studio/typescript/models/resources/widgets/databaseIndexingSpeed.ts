/// <reference path="../../../../typings/tsd.d.ts" />

import indexingSpeedItem = require("models/resources/widgets/indexingSpeedItem");

class databaseIndexingSpeed {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);
    
    items: indexingSpeedItem[] = [];

    constructor(tag: string) {
        this.tag = tag;
    }
    
    update(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseIndexingSpeedPayload) {
        this.items = data.Items.map(x => new indexingSpeedItem(nodeTag, x));
    }
}

export = databaseIndexingSpeed;
