import numberFormattingStorage = require("common/numberFormattingStorage");

class settings {
    static useRawFormat: boolean = numberFormattingStorage.shouldUseRaw();

    static watchStorage() {
        $(window).bind("storage", (e: any) => {
            if (e.originalEvent.key === numberFormattingStorage.localStorageName) {
                settings.useRawFormat = e.originalEvent.newValue;
            }
        });
    }
}

settings.watchStorage();

export = settings;
