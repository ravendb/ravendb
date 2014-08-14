class serverLogsConfigEntry {

    category = ko.observable<string>();
    level = ko.observable<string>();

    constructor(category: string, level: string) {
        this.category(category);
        this.level(level);
    }

    static empty() {
        return new serverLogsConfigEntry(null, null);
    }

    toDto(): serverLogsConfigEntryDto {
        return {
            category: this.category(),
            level: this.level()
        }
    }
}

export = serverLogsConfigEntry;