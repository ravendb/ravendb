/// <reference path="../../../typings/tsd.d.ts" />

import studioSetting = require("common/settings/studioSetting");

class dontShowAgainSettings extends studioSetting<Array<studio.settings.dontShowAgain>> {

    readonly name = "dontShowAgain";

    constructor() {
        super([]);
    }

    shouldShow(type: studio.settings.dontShowAgain): boolean {
        return _.includes(this.value, type);
    }

    ignore(type: studio.settings.dontShowAgain): void {
        if (!this.shouldShow(type)) {
            this.value.push(type);
        }
    }

    resetAll(): void {
        this.value = [];
    }
}

export = dontShowAgainSettings;
