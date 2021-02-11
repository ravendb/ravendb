/// <reference path="../../../typings/tsd.d.ts" />

class storageKeyProvider {

    static commonPrefix = "ravedb-5.2-";

    static storageKeyFor(value: string) {
        return storageKeyProvider.commonPrefix + value;
    }

}

export = storageKeyProvider;
