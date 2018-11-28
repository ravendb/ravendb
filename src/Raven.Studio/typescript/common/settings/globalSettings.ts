/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");
import abstractSettings = require("common/settings/abstractSettings");
import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import dontShowAgainSettings = require("common/settings/dontShowAgainSettings");
import studioSetting = require("common/settings/studioSetting");

class globalSettings extends abstractSettings {
    private readonly remoteSettingsLoader: () => JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>;
    private readonly remoteSettingsSaver: (settings: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration) => JQueryPromise<void>;

    replicationFactor = new simpleStudioSetting<number>(
        "remote", null, x => this.saveSetting(x));
    
    numberFormatting = new simpleStudioSetting<studio.settings.numberFormatting>("local", "formatted", x => this.saveSetting(x));
    dontShowAgain = new dontShowAgainSettings(x => this.saveSetting(x));
    sendUsageStats = new simpleStudioSetting<boolean | undefined>("local", undefined, x => this.saveSetting(x));

    feedback = new simpleStudioSetting<feedbackSavedSettingsDto>("local", null, x => this.saveSetting(x));

    constructor(remoteSettingsLoader: () => JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration>,
                remoteSettingsSaver: (settings: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration) => JQueryPromise<void>, 
                onSettingChanged: (key: string, value: studioSetting<any>) => void) {
        super(onSettingChanged);
        this.remoteSettingsLoader = remoteSettingsLoader;
        this.remoteSettingsSaver = remoteSettingsSaver;
    }

    get storageKey() {
        return storageKeyProvider.storageKeyFor("settings");
    }

    protected fetchConfigDocument(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration> {
        if (this.remoteSettingsLoader == null) {
            throw new Error("Global settings loader not found");
        }
        return this.remoteSettingsLoader();
    }

    protected saveConfigDocument(settingsToSave: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration): JQueryPromise<void> {
        return this.remoteSettingsSaver(settingsToSave);
    }
}

export = globalSettings;
