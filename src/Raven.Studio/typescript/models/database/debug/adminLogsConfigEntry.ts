/// <reference path="../../../../typings/tsd.d.ts"/>

type logEntryMode = "include" | "exclude";



class adminLogsConfigEntry {
    headerName = ko.observable<adminLogsHeaderType>("Source");
    headerValue = ko.observable<string>();
    mode = ko.observable<logEntryMode>();

    constructor(headerName: adminLogsHeaderType, headerValue: string, mode: logEntryMode) {
        this.headerName(headerName);
        this.headerValue(headerValue);
        this.mode(mode);
    }

    clone() {
        return new adminLogsConfigEntry(this.headerName(), this.headerValue(), this.mode());
    }
    
    toFilter() {
        return this.headerName() + ":" + this.headerValue();
    }

}

export = adminLogsConfigEntry;
