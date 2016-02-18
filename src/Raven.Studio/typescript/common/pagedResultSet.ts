/// <reference path="../../typings/tsd.d.ts" />

class pagedResultSet<T> {
    constructor(public items: Array<T>, public totalResultCount: number, public additionalResultInfo?: any) {
    }
}

export = pagedResultSet;
