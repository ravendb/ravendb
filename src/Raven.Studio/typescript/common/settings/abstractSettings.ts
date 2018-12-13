/// <reference path="../../../typings/tsd.d.ts" />

import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import studioSetting = require("common/settings/studioSetting");


abstract class abstractSettings {

    environment = new simpleStudioSetting<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>(
        "remote", "None", x => this.saveSetting(x));
    
    disabled = new simpleStudioSetting<boolean>(
        "remote", false, x => this.saveSetting(x));

    abstract get storageKey(): string;
    protected abstract fetchConfigDocument(): JQueryPromise<any>;
    protected abstract saveConfigDocument(settingsToSave: any): JQueryPromise<void>;
    protected readonly onSettingChanged: (key: string, setting: studioSetting<any>) => void;

    protected constructor(onSettingChanged: (key: string, value: any) => void) {
        this.onSettingChanged = onSettingChanged;
    }
    
    private serializeSettings(location: studio.settings.saveLocation) {
        const settings = {} as any;
        
        _.forIn(this, (value, name) => {
            if (value instanceof studioSetting && value.saveLocation === location) {
                settings[studioSetting.propertyNameInStorage(name, location)] = value.prepareValueForSave();
            }
        });
        
        return settings;
    }
    
    protected findPropertyName(item: studioSetting<any>): string {
        let settingName: string = null;

        _.forIn(this, (value, name) => {
            if (value instanceof studioSetting && value.saveLocation === item.saveLocation && value === item) {
                settingName = name;
            }
        });
        
        return settingName;
    }

    protected saveSetting(item: studioSetting<any>): JQueryPromise<void> {
        const settings = this.serializeSettings(item.saveLocation);
        const settingName = this.findPropertyName(item);

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
                if (value.saveLocation === "remote") {
                    value.loadUsingValue(remoteSettings ? remoteSettings[_.upperFirst(name)] : undefined);
                } else {
                    value.loadUsingValue(localSettings ? localSettings[name] : undefined);
                }
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

    /**
     * Force saving local & remote settings
     */
    save(): JQueryPromise<void> {
        const remoteSettings = this.serializeSettings("remote");
        const remoteTask = this.saveConfigDocument(remoteSettings);
        
        const localSettings = this.serializeSettings("local");
        const localTask = this.saveLocalSettings(localSettings);

        // notify handlers that settings may change
        _.forIn(this, (value, name) => {
            if (value instanceof studioSetting) {
                this.onSettingChanged(name, value);
            }
        });
        
        return $.when<void>(remoteTask, localTask);
    }
}

export = abstractSettings;
