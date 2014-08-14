import serverLogsConfigEntry = require("models/serverLogsConfigEntry");

class serverLogsConfig {
    entries = ko.observableArray<serverLogsConfigEntry>();
    maxEntries = ko.observable<number>();

    clone(): serverLogsConfig {
        var newConfig = new serverLogsConfig();
        newConfig.maxEntries(this.maxEntries());
        newConfig.entries($.map(this.entries() || [], (e, idx) => e.clone()));
        return newConfig;
    }
}

export = serverLogsConfig;