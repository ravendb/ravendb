class adminLogsConfigEntry {

    category = ko.observable<string>();
    level = ko.observable<string>();

    constructor(category: string, level: string) {
        this.category(category);
        this.level(level);
    }

    clone() {
        return new adminLogsConfigEntry(this.category(), this.level());
    }

    static empty() {
        return new adminLogsConfigEntry(null, null);
    }

    toDto(): adminLogsConfigEntryDto {
        return {
            category: this.category(),
            level: this.level()
        }
    }
}

export = adminLogsConfigEntry;