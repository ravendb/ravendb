/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");

class serverSmugglingLocalStorage {

    static localStorageName = storageKeyProvider.storageKeyFor("serverSmuggling");

    static get(): serverSmugglingDto {
        return localStorage.getObject(serverSmugglingLocalStorage.localStorageName);
    }

    static setValue(value: serverSmugglingDto) {
        localStorage.setObject(serverSmugglingLocalStorage.localStorageName, value);
    }

    static clean() {
        localStorage.removeItem(serverSmugglingLocalStorage.localStorageName);
    }

}

export = serverSmugglingLocalStorage;
