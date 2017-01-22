/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");

class apiKeyLocalStorage {

    static localStorageName = storageKeyProvider.storageKeyFor("apiKey");

    static get() {
        return localStorage.getObject(apiKeyLocalStorage.localStorageName);
    }

    static setValue(apiKey: string) {
        localStorage.setObject(apiKeyLocalStorage.localStorageName, apiKey);
    }

    static clean() {
        localStorage.removeItem(apiKeyLocalStorage.localStorageName);
    }

    static notifyAboutLogOut() {
        const event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, apiKeyLocalStorage.localStorageName, null, null, null, window.localStorage);
        window.dispatchEvent(event);
    }
}

export = apiKeyLocalStorage;
