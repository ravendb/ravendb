/// <reference path="../../../typings/tsd.d.ts" />

import studioSetting = require("common/settings/studioSetting");

class simpleStudioSetting<T> extends studioSetting<T> {
    name: string;
    private readonly localSetting: boolean;

    constructor(defaultValue: T, name: string) {
        super(defaultValue);
        this.name = name;
    }

    getValue(): T {
        return this.value;
    }

    setValue(value: T) {
        this.value = value;
    }
}

export = simpleStudioSetting;
