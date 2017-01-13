/// <reference path="../../../typings/tsd.d.ts"/>

class numberFormattingStorage {

    static localStorageName = "NumberFormatting";

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
