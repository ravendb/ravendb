/// <reference path="../../typings/tsd.d.ts" />

// we use this function wrapper as knockout calls functions stored directly in observableArray
class changesCallback<T> {

    constructor(private onFire: (arg: T) => void) {

    }

    fire(arg: T) {
        this.onFire(arg);
    }

}

export = changesCallback;
