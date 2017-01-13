/// <reference path="../../../typings/tsd.d.ts" />

class serverSmugglingLocalStorage {

    static localStorageName = "serverSmuggling";

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
