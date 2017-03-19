/// <reference path="../../../typings/tsd.d.ts" />

abstract class studioSetting<T> {
    protected value: T;
    protected readonly defaultValue: T;

    constructor(defaultValue: T) {
        this.defaultValue = defaultValue;
    }

    abstract get name(): string;

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
