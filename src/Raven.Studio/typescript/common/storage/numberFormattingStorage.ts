/// <reference path="../../../typings/tsd.d.ts"/>

import storageKeyProvider = require("common/storage/storageKeyProvider");

//TODO: deprecated - use global settings
class numberFormattingStorage {

    static localStorageName = storageKeyProvider.storageKeyFor("numberFormatting");

    static shouldUseRaw() {
        return localStorage.getObject(numberFormattingStorage.localStorageName);
    }

    static save(useRawFormat: boolean) {
        if (useRawFormat) {
            localStorage.setObject(numberFormattingStorage.localStorageName, true);
        } else {
            localStorage.removeItem(numberFormattingStorage.localStorageName);
        }

        const event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, numberFormattingStorage.localStorageName, useRawFormat, !useRawFormat, null, window.sessionStorage);
        window.dispatchEvent(event);
    }

}

export = numberFormattingStorage;
