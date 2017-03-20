/// <reference path="../../../typings/tsd.d.ts" />

import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import studioSetting = require("common/settings/studioSetting");

abstract class abstractSettings {

    environment = new simpleStudioSetting<studio.settings.usageEnvironment>("Default", "environment");

    abstract get storageKey(): string;
    protected abstract fetchConfigDocument(): JQueryPromise<any>;
    protected abstract saveConfigDocument(settingsToSave: any): JQueryPromise<void>;

    protected readSettings(remoteSettings: any, localSettings: any) {
        this.readSetting(remoteSettings, this.environment);
    }

    protected writeSettings(remoteSettings: any, localSettings: any) {
        this.writeSetting(remoteSettings, this.environment);
    }

    protected readSetting(source: any, targetSetting: studioSetting<any>) {
        targetSetting.deserialize(source ? source[targetSetting.name] : undefined);
    }

    protected writeSetting(target: any, settingToWrite: studioSetting<any>) {
        target[settingToWrite.name] = settingToWrite.serialize();
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

    save(): JQueryPromise<void> {
        const localSettings = {} as any;
        const remoteSettings = {} as any;

        //TODO: what about concurrent updates?

        this.writeSettings(remoteSettings, localSettings);

        localStorage.setItem(this.storageKey, localSettings);

        return this.saveConfigDocument(remoteSettings);
    }
}

export = abstractSettings;
