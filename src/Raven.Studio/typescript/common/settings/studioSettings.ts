/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import globalSettings = require("common/settings/globalSettings");
import databaseSettings = require("common/settings/databaseSettings");

class studioSettings {
    private static globalSettingsCached: JQueryPromise<globalSettings>;

    static forDatabase(db: database): JQueryPromise<databaseSettings> {
        const settings = new databaseSettings(db);

        return settings.load();
    }

    static globalSettings(): JQueryPromise<globalSettings> {
        if (!this.globalSettingsCached) {
            const global = new globalSettings();

            this.globalSettingsCached = global.load();
        }

        return this.globalSettingsCached;
    }
}

export = studioSettings;
