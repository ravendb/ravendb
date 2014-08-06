import customLogEntry = require("models/customLogEntry");

class customLogConfig {
    entries = ko.observableArray<customLogEntry>();
    maxEntries = ko.observable<number>();
}

export = customLogConfig;