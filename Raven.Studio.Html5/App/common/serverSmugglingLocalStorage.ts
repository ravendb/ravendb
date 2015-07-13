/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

class serverSmugglingLocalStorage {

    public static localStorageName = "serverSmuggling";

    public static get(): serverSmugglingDto {
        return localStorage.getObject(serverSmugglingLocalStorage.localStorageName);
    }

    public static setValue(value: serverSmugglingDto) {
        localStorage.setObject(serverSmugglingLocalStorage.localStorageName, value);
    }

    public static clean() {
        localStorage.removeItem(serverSmugglingLocalStorage.localStorageName);
    }

}

export = serverSmugglingLocalStorage;