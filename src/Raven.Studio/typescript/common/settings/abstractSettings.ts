/// <reference path="../../../typings/tsd.d.ts" />

import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import studioSetting = require("common/settings/studioSetting");


abstract class abstractSettings {

    environment = new simpleStudioSetting<studio.settings.usageEnvironment>("remote", "Default", x => this.saveSetting(x));

    abstract get storageKey(): string;
    protected abstract fetchConfigDocument(): JQueryPromise<any>;
    protected abstract saveConfigDocument(settingsToSave: any): JQueryPromise<void>;
    protected readonly onSettingChanged: (key: string, setting: studioSetting<any>) => void;

    constructor(onSettingChanged: (key: string, value: any) => void) {
        this.onSettingChanged = onSettingChanged;
    }

    protected saveSetting(item: studioSetting<any>): JQueryPromise<void> {

        const settings = {} as any;
        let settingName: string;

        _.forIn(this, (value, name) => {
            if (value instanceof studioSetting && value.saveLocation === item.saveLocation) {
                settings[name] = value.serialize();

                if (value === item) {
                    settingName = name;
                }
            }
        });

        this.onSettingChanged(settingName, item);

        switch (item.saveLocation) {
            case "local":
                return this.saveLocalSettings(settings);
            case "remote":
                return this.saveConfigDocument(settings);
            default:
                throw new Error("Unhandled save location:" + item.saveLocation);
        }
    }

    private saveLocalSettings(localSettings: any) {
        localStorage.setObject(this.storageKey, localSettings);
        return $.Deferred<void>().resolve();
    }
    
    protected readSettings(remoteSettings: any, localSettings: any) {
        _.forIn(this, (value, name) => {
            if (value instanceof studioSetting) {
                const sourceItem = value.saveLocation === "remote" ? remoteSettings : localSettings;
                value.deserialize(sourceItem ? sourceItem[name] : undefined);
            }
        });
    }

    load(): JQueryPromise<this> {
        const loadTask = $.Deferred<this>();

        const localSettings = localStorage.getObject(this.storageKey);

        this.fetchConfigDocument()
            .done((remoteSettings) => {
                this.readSettings(remoteSettings, localSettings);
                loadTask.resolve(this);
            })
            .fail(() => loadTask.reject());

        return loadTask;
    }
}

export = abstractSettings;
