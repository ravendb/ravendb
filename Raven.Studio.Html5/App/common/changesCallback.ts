/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

// we use this function wrapper as knockout calls functions stored directly in observableArray
class changesCallback<T> {

    constructor(private onFire: (T) => void) {

    }

    fire(arg: T) {
        this.onFire(arg);
    }

}

export = changesCallback;