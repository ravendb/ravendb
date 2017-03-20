/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");
import abstractSettings = require("common/settings/abstractSettings");
import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import dontShowAgainSettings = require("common/settings/dontShowAgainSettings");

class globalSettings extends abstractSettings {
    numberFormatting = new simpleStudioSetting<studio.settings.numberFormatting>("formatted", "numberFormatting");
    dontShowAgain = new dontShowAgainSettings();
    sendUsageStats = new simpleStudioSetting<boolean>(false, "sendUsageStats");

    get storageKey() {
        return storageKeyProvider.storageKeyFor("settings");
    }

    protected readSettings(remoteSettings: any, localSettings: any) {
        super.readSettings(remoteSettings, localSettings);

        this.readSetting(localSettings, this.numberFormatting);
        this.readSetting(localSettings, this.dontShowAgain);
        this.readSetting(localSettings, this.sendUsageStats);
    }

    protected writeSettings(remoteSettings: any, localSettings: any) {
        super.writeSettings(remoteSettings, localSettings);

        this.writeSetting(localSettings, this.numberFormatting);
        this.writeSetting(localSettings, this.dontShowAgain);
        this.writeSetting(localSettings, this.sendUsageStats);
    }

    protected fetchConfigDocument(): JQueryPromise<any> {
        //TODO:
        return $.Deferred<any>().resolve(undefined);
    }

    protected saveConfigDocument(settingsToSave: any): JQueryPromise<void> {
        //TODO: 
        const task = $.Deferred<void>();
        setTimeout(() => task.resolve(), 10);
        return task;
    }
}

export = globalSettings;
