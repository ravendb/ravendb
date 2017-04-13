/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import globalSettings = require("common/settings/globalSettings");
import databaseSettings = require("common/settings/databaseSettings");
import abstractSettings = require("common/settings/abstractSettings");
import studioSetting = require("common/settings/studioSetting");

class studioSettings {

    static default = new studioSettings();

    private globalSettingsCached: JQueryPromise<globalSettings>;

    forDatabase(db: database): JQueryPromise<databaseSettings> {
        const settings = new databaseSettings((key, value) => this.onSettingChanged(key, value), db);

        return settings.load();
    }

    globalSettings(): JQueryPromise<globalSettings> {
        if (!this.globalSettingsCached) {
            const global = new globalSettings((key, value) => this.onSettingChanged(key, value));

            this.globalSettingsCached = global.load();
        }

        return this.globalSettingsCached;
    }

    init(currentDatabaseSettings: KnockoutObservable<databaseSettings>) {
        window.addEventListener("storage", e => {
            this.globalSettings()
                .done(global => {
                    if (e.key === global.storageKey && e.newValue) {
                        this.onSettingsChanged(global, JSON.parse(e.newValue));
                    }
                });

            const dbSettings = currentDatabaseSettings();
            if (e.key === dbSettings.storageKey && e.newValue) {
                this.onSettingsChanged(dbSettings, JSON.parse(e.newValue));
            }
        });
    }

    private onSettingsChanged(container: abstractSettings, settingsDto: any) {
        _.forIn(settingsDto, (value, key) => {
            const setting = (container as any)[key];
            if (setting && setting instanceof studioSetting && setting.serialize() !== value) {
                setting.deserialize(value);
                this.onSettingChanged(key, setting);
            }
        });
    }

    private onSettingChanged(name: string, setting: studioSetting<any>) {
        //TODO: dispatch to event listeners
        console.log("changed: " + name + ", value = " + setting.serialize());
    }

}

export = studioSettings;
