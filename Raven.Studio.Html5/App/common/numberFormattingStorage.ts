/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

class numberFormattingStorage {

    public static localStorageName = "NumberFormatting";

    public static shouldUseRaw() {
        return localStorage.getObject(numberFormattingStorage.localStorageName);
    }

    public static save(useRawFormat: boolean) {
        if (useRawFormat) {
            localStorage.setObject(numberFormattingStorage.localStorageName, true);
        } else {
            localStorage.removeItem(numberFormattingStorage.localStorageName);
        }

        var event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, numberFormattingStorage.localStorageName, useRawFormat, !useRawFormat, null, window.sessionStorage);
        window.dispatchEvent(event);
    }
}

export = numberFormattingStorage;
