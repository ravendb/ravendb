/// <reference path="../../typings/tsd.d.ts" />
class eventSourceSettingStorage {

    public static localStorageName = "EventSourceDisabled";

    public static get() {
        return localStorage.getObject(eventSourceSettingStorage.localStorageName);
    }

    public static setValue(disabled: boolean) {
        if (disabled) {
            localStorage.setObject(eventSourceSettingStorage.localStorageName, true);
        } else {
            localStorage.removeItem(eventSourceSettingStorage.localStorageName);
        }

        var event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, eventSourceSettingStorage.localStorageName, disabled, !disabled, null, window.sessionStorage);
        window.dispatchEvent(event);
    }

    public static useEventSource(): boolean {
        var disabled = !!eventSourceSettingStorage.get();
        return !disabled;
    }
}

export = eventSourceSettingStorage;
