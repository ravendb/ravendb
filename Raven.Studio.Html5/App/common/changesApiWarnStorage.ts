/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

class changesApiWarnStorage {

    public static localStorageName = "ChangesApiWarnDisabled";

    public static get() {
        return localStorage.getObject(changesApiWarnStorage.localStorageName);
    }

    public static setValue(disabled: boolean) {
        if (disabled) {
            localStorage.setObject(changesApiWarnStorage.localStorageName, true);
        } else {
            localStorage.removeItem(changesApiWarnStorage.localStorageName);
        }
    }

    public static showChangesApiWarning(): boolean {
        return !changesApiWarnStorage.get();
    }
}

export = changesApiWarnStorage;
