/// <reference path="../../../typings/tsd.d.ts" />

abstract class studioSetting<T> {
    protected value: T;
    protected readonly defaultValue: T;
    protected saveHandler: (item: this) => JQueryPromise<void>; 
    readonly saveLocation: studio.settings.saveLocation;

    constructor(saveLocation: studio.settings.saveLocation, defaultValue: T, saveHandler: (item: studioSetting<T>) => JQueryPromise<void>) {
        this.saveLocation = saveLocation;
        this.defaultValue = defaultValue;
        this.saveHandler = saveHandler;
    }

    protected save() {
        return this.saveHandler(this);
    }

    serialize() {
        return JSON.stringify(this.value);
    }

    deserialize(json: any) {
        if (_.isUndefined(json)) {
            this.value = this.defaultValue;
            return;
        }
        this.value = JSON.parse(json);
    }
}

export = studioSetting;
