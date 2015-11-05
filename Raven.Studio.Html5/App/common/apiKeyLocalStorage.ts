/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

class apiKeyLocalStorage {

    public static localStorageName = "apiKey";

    public static get() {
        return localStorage.getObject(apiKeyLocalStorage.localStorageName);
    }

    public static setValue(apiKey: string) {
        localStorage.setObject(apiKeyLocalStorage.localStorageName, apiKey);
    }

    public static clean() {
        localStorage.removeItem(apiKeyLocalStorage.localStorageName);
    }

    public static notifyAboutLogOut() {
        var event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, apiKeyLocalStorage.localStorageName, null, null, null, window.localStorage);
        window.dispatchEvent(event);
    }
}

export = apiKeyLocalStorage;
