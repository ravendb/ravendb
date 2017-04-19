/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import globalSettings = require("common/settings/globalSettings");
import databaseSettings = require("common/settings/databaseSettings");
import abstractSettings = require("common/settings/abstractSettings");
import studioSetting = require("common/settings/studioSetting");

type handlerItem = {
    nameCondition: (name: string) => boolean;
    handler: (name: string, setting: studioSetting<any>) => void;
}

class studioSettings {

    static default = new studioSettings();

    private globalSettingsCached: JQueryPromise<globalSettings>;
    private readonly onSettingChangedHandlers = [] as Array<handlerItem>;

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
        this.onSettingChangedHandlers.forEach(item => {
            if (item.nameCondition(name)) {
                item.handler(name, setting);
            }
        });
    }

    registerOnSettingChangedHandler<T extends studioSetting<any>>(nameCondition: (name: string) => boolean, handler: (name: string, setting: T) => void): disposable {
        const entry = {
            nameCondition,
            handler
        } as handlerItem;

        this.onSettingChangedHandlers.push(entry);

        return {
            dispose: () => _.pull(this.onSettingChangedHandlers, entry)
        }
    }

}

export = studioSettings;
