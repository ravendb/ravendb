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

    private globalRemoteSettingsLoader: () => JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>;
    private databaseRemoteSettingsLoader: (db: database) => JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>;
    private globalRemoteSettingsSaver: (settings: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration) => JQueryPromise<void>;
    private databaseRemoteSettingsSaver: (settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration, db: database) => JQueryPromise<void>;
    
    private globalSettingsCached: JQueryPromise<globalSettings>;
    private readonly onSettingChangedHandlers = [] as Array<handlerItem>;

    
    configureLoaders(globalLoader:  () => JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>, 
                     dbSpecificLoader: (db: database) => JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>,
                     globalSaver: (settings: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration) => JQueryPromise<void>,
                     dbSpecificSaver: (settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration, db: database) => JQueryPromise<void>) {
        this.globalRemoteSettingsLoader = globalLoader;
        this.databaseRemoteSettingsLoader = dbSpecificLoader;
        this.globalRemoteSettingsSaver = globalSaver;
        this.databaseRemoteSettingsSaver = dbSpecificSaver;
    }
    
    forDatabase(db: database): JQueryPromise<databaseSettings> {
        const settings = new databaseSettings(this.databaseRemoteSettingsLoader, (key, value) => this.onSettingChanged(key, value), db);

        return settings.load();
    }

    globalSettings(forceRefresh = false): JQueryPromise<globalSettings> {
        if (forceRefresh && this.globalSettingsCached && this.globalSettingsCached.state() !== "pending") {
            this.globalSettingsCached = null;
        }
        
        if (!this.globalSettingsCached) {
            const global = new globalSettings(this.globalRemoteSettingsLoader, this.globalRemoteSettingsSaver, (key, value) => this.onSettingChanged(key, value));

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
            if (dbSettings && e.key === dbSettings.storageKey && e.newValue) {
                this.onSettingsChanged(dbSettings, JSON.parse(e.newValue));
            }
        });
    }

    private onSettingsChanged(container: abstractSettings, settingsDto: any) {
        _.forIn(settingsDto, (value, key) => {
            const setting = (container as any)[key];
            if (setting && setting instanceof studioSetting && setting.prepareValueForSave() !== value) {
                setting.loadUsingValue(value);
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
