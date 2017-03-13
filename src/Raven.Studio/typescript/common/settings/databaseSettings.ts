/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import abstractSettings = require("common/settings/abstractSettings");

class databaseSettings extends abstractSettings {
    private db: database;

    constructor(db: database) {
        super();
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
        //TODO: 
        const task = $.Deferred<void>();
        setTimeout(() => task.resolve(), 10);
        return task;
    }

    protected readSettings(remoteSettings: any, localSettings: any) {
        super.readSettings(remoteSettings, localSettings);
    }

    protected writeSettings(remoteSettings: any, localSettings: any) {
        super.writeSettings(remoteSettings, localSettings);
    }

}

export = databaseSettings;
