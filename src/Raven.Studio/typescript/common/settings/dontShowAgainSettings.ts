/// <reference path="../../../typings/tsd.d.ts" />

import studioSetting = require("common/settings/studioSetting");

class dontShowAgainSettings extends studioSetting<Array<studio.settings.dontShowAgain>> {

    constructor(saveHandler: (item: dontShowAgainSettings) => JQueryPromise<void>) {
        super("local", [], saveHandler);
    }

    shouldShow(type: studio.settings.dontShowAgain): boolean {
        return !_.includes(this.value, type);
    }

    ignore(type: studio.settings.dontShowAgain) {
        if (this.shouldShow(type)) {
            this.value.push(type);
        }

        return this.save();
    }

    resetAll() {
        this.value = [];

        return this.save();
    }
}

export = dontShowAgainSettings;
