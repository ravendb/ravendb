/// <reference path="../../../../typings/tsd.d.ts" />

import trafficWatchItem = require("models/resources/widgets/trafficWatchItem");

class databaseTrafficWatch {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    items: trafficWatchItem[] = [];

    constructor(tag: string) {
        this.tag = tag;
    }

    update(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload) {
        this.items = data.Items.map(x => new trafficWatchItem(nodeTag, x));
    }
}

export = databaseTrafficWatch;
