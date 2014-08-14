import serverLogsConfigEntry = require("models/serverLogsConfigEntry");

class serverLogsConfig {
    entries = ko.observableArray<serverLogsConfigEntry>();
    maxEntries = ko.observable<number>();
}

export = serverLogsConfig;