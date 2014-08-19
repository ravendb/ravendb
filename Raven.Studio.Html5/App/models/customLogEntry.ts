class customLogEntry {

    category = ko.observable<string>();
    level = ko.observable<string>();

    constructor(category: string, level: string) {
        this.category(category);
        this.level(level);
    }

    static empty() {
        return new customLogEntry(null, null);
    }

    toDto(): customLogEntryDto {
        return {
            category: this.category(),
            level: this.level()
        }
    }
}

export = customLogEntry;