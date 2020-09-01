/// <reference path="../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import abstractSettings = require("common/settings/abstractSettings");
import studioSetting = require("common/settings/studioSetting");

class databaseSettings extends abstractSettings {
    private readonly db: database;
    private readonly remoteSettingsLoader: (db: database) => JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>;
    private readonly remoteSettingsSaver: (settings: Raven.Client.Documents.Operations.Configuration.StudioConfiguration, db: database) => JQueryPromise<void>;

    constructor(removeSettingsLoader: (db: database) => JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>,
                onSettingChanged: (key: string, value: studioSetting<any>) => void,
                db: database) {
        super(onSettingChanged);
        this.db = db;
        this.remoteSettingsLoader = removeSettingsLoader;
    }

    get storageKey() {
        return storageKeyProvider.storageKeyFor("settings." + this.db.name);
    }

    protected fetchConfigDocument(): JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration> {
        if (this.remoteSettingsLoader == null) {
            throw new Error("Database settings loader not found");
        }
        return this.remoteSettingsLoader(this.db);
    }

    protected saveConfigDocument(settingsToSave: any): JQueryPromise<void> {
        return this.remoteSettingsSaver(settingsToSave, this.db);
    }
}

export = databaseSettings;
