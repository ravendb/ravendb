/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import abstractSettings = require("common/settings/abstractSettings");
import studioSetting = require("common/settings/studioSetting");

class databaseSettings extends abstractSettings {
    private db: database;

    constructor(onSettingChanged: (key: string, value: studioSetting<any>) => void, db: database) {
        super(onSettingChanged);
        this.db = db;
    }

    get storageKey() {
        //TODO: register for disposal, after db is deleted
        return storageKeyProvider.storageKeyFor("settings." + this.db.name);
    }

    protected fetchConfigDocument(): JQueryPromise<any> {
        //TODO:
        return $.Deferred<any>().resolve(undefined);
    }

    protected saveConfigDocument(settingsToSave: any): JQueryPromise<void> {
        throw new Error("not yet implemented");
    }

}

export = databaseSettings;
