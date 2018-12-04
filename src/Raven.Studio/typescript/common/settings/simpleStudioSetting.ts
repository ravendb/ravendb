/// <reference path="../../../typings/tsd.d.ts" />

import studioSetting = require("common/settings/studioSetting");

class simpleStudioSetting<T> extends studioSetting<T> {

    constructor(saveLocation: studio.settings.saveLocation, defaultValue: T, saveHandler: (item: simpleStudioSetting<T>) => JQueryPromise<void>) {
        super(saveLocation, defaultValue, saveHandler);
    }

    getValue(): T {
        return this.value;
    }

    setValue(value: T) {
        this.setValueLazy(value);

        return this.save();
    }
    
    setValueLazy(value: T) {
        this.value = value;
    }
}

export = simpleStudioSetting;
