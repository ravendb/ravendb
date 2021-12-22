/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");
import moment = require("moment");

class serverBuildReminder {

    static localStorageName = storageKeyProvider.storageKeyFor("lastServerBuildReminder");

    static get() {
        return localStorage.getObject(serverBuildReminder.localStorageName);
    }

    static mute(isMuteNeeded: boolean) {
        if (isMuteNeeded) {
            localStorage.setObject(serverBuildReminder.localStorageName, new Date());
        } else {
            localStorage.removeItem(serverBuildReminder.localStorageName);
        }

        const event: any = document.createEvent('StorageEvent');
        event.initStorageEvent('storage', false, false, serverBuildReminder.localStorageName, isMuteNeeded, !isMuteNeeded, null, window.sessionStorage);
        window.dispatchEvent(event);
    }

    static isReminderNeeded(): boolean {
        const lastBuildCheck = serverBuildReminder.get();
        const timestamp = Date.parse(lastBuildCheck);
        let difference = 0;

        if (!isNaN(timestamp)) {
            const lastBuildCheckMoment = moment(lastBuildCheck);
            const currentDateMoment = moment(new Date());
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
