/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");
import abstractSettings = require("common/settings/abstractSettings");
import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import dontShowAgainSettings = require("common/settings/dontShowAgainSettings");
import studioSetting = require("common/settings/studioSetting");

class globalSettings extends abstractSettings {
    numberFormatting = new simpleStudioSetting<studio.settings.numberFormatting>("local", "formatted", x => this.saveSetting(x));
    dontShowAgain = new dontShowAgainSettings(x => this.saveSetting(x));
    sendUsageStats = new simpleStudioSetting<boolean>("local", false, x => this.saveSetting(x));

    feedback = new simpleStudioSetting<feedbackSavedSettingsDto>("local", null, x => this.saveSetting(x));

    constructor(onSettingChanged: (key: string, value: studioSetting<any>) => void) {
        super(onSettingChanged);
    }

    get storageKey() {
        return storageKeyProvider.storageKeyFor("settings");
    }

    protected fetchConfigDocument(): JQueryPromise<any> {
        //TODO:
        return $.Deferred<any>().resolve(undefined);
    }

    protected saveConfigDocument(settingsToSave: any): JQueryPromise<void> {
        throw new Error("not yet implemented");
    }
}

export = globalSettings;
