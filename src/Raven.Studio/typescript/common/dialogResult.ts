/// <reference path="../../typings/tsd.d.ts" />

class dialogResult {
    constructor(public cancelled: boolean, public task: JQueryPromise<any>) {
    }
}

export = dialogResult;
