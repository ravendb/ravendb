class adminLogsConfigEntry {
    includeStackTrace = ko.observable<boolean>();
    category = ko.observable<string>();
    level = ko.observable<string>();

    constructor(category: string, level: string, includeStackTrace: boolean = false) {
        this.category(category);
        this.level(level);
        this.includeStackTrace(includeStackTrace);
    }

    clone() {
        return new adminLogsConfigEntry(this.category(), this.level(), this.includeStackTrace());
    }

    static empty() {
        return new adminLogsConfigEntry(null, null, false);
    }

    toDto(): adminLogsConfigEntryDto {
        return {
            category: this.category(),
            level: this.level(),
            includeStackTrace: this.includeStackTrace()
        }
    }
}

export = adminLogsConfigEntry;
