/// <reference path="../../../../typings/tsd.d.ts"/>

type logEntryMode = "include" | "exclude";

class adminLogsConfigEntry {
    logSource = ko.observable<string>();
    mode = ko.observable<logEntryMode>();

    constructor(logSource: string, mode: logEntryMode) {
        this.logSource(logSource);
        this.mode(mode);
    }

    clone() {
        return new adminLogsConfigEntry(this.logSource(), this.mode());
    }

}

export = adminLogsConfigEntry;
