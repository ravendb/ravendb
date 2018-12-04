/// <reference path="../../../typings/tsd.d.ts" />

abstract class studioSetting<T> {
    protected value: T;
    protected readonly defaultValue: T;
    protected saveHandler: (item: this) => JQueryPromise<void>; 
    readonly saveLocation: studio.settings.saveLocation;

    protected constructor(saveLocation: studio.settings.saveLocation, defaultValue: T, saveHandler: (item: studioSetting<T>) => JQueryPromise<void>) {
        this.saveLocation = saveLocation;
        this.defaultValue = defaultValue;
        this.saveHandler = saveHandler;
    }

    protected save(): JQueryPromise<void> {
        return this.saveHandler(this);
    }

    prepareValueForSave() {
        if (this.saveLocation === "local") {
            return JSON.stringify(this.value);
        } else {
            return this.value;
        }
    }
    
    static propertyNameInStorage(propertyName: string, location: studio.settings.saveLocation) {
        return location === "local" ? propertyName : _.upperFirst(propertyName);
    }

    loadUsingValue(value: any) {
        if (this.saveLocation === "local") {
            if (_.isUndefined(value)) {
                this.value = this.defaultValue;
                return;
            }
            this.value = JSON.parse(value);
        } else {
            if (_.isUndefined(value)) {
                this.value = this.defaultValue;
            } else {
                this.value = value;    
            }
            
        }
    }
}

export = studioSetting;
