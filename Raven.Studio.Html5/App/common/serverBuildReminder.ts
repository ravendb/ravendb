/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

class serverBuildReminder {

    public static localStorageName = "LastServerBuildReminder";

    public static get() {
        return localStorage.getObject(serverBuildReminder.localStorageName);
    }

    public static mute(isMuteNeeded: boolean) {
        if (isMuteNeeded) {
            localStorage.setObject(serverBuildReminder.localStorageName, new Date());
        }
        else {
            localStorage.removeItem(serverBuildReminder.localStorageName);
        }

        var event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, serverBuildReminder.localStorageName, isMuteNeeded, !isMuteNeeded, null, window.sessionStorage);
        window.dispatchEvent(event);
    }

    public static isReminderNeeded(): boolean {
        var lastBuildCheck = serverBuildReminder.get();
        var timestamp = Date.parse(lastBuildCheck);
        var difference = 0;

        if (!isNaN(timestamp)) {
            var lastBuildCheckMoment = moment(lastBuildCheck);
            var currentDateMoment = moment(new Date());
            difference = currentDateMoment.diff(lastBuildCheckMoment, 'days', true);
        }

        if (isNaN(timestamp) || difference > 7) { //more than a week
            serverBuildReminder.mute(false);
            return true;
        }

        return false;
    }
}

export = serverBuildReminder;