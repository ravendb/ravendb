/// <reference path="../../../../typings/tsd.d.ts"/>

import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");

class adminLogsConfig {
    entries = ko.observableArray<adminLogsConfigEntry>([]);
    maxEntries = ko.observable<number>();

    copyTo(targetConfig: adminLogsConfig) {
        targetConfig.maxEntries(this.maxEntries());
        targetConfig.entries(this.entries().map(x => x.clone()));
    }
    
    static empty() {
        const config = new adminLogsConfig();
        config.maxEntries(100000);
        return config;
    }
}

export = adminLogsConfig;
