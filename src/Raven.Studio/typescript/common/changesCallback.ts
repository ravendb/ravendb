/// <reference path="../../typings/tsd.d.ts" />

// we use this function wrapper as knockout calls functions stored directly in observableArray
class changesCallback<T> {

    private onFire: (arg: T) => void;

    constructor(onFire: (arg: T) => void) {
        this.onFire = onFire;
    }

    fire(arg: T) {
        this.onFire(arg);
    }

}

export = changesCallback;
