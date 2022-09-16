/// <reference path="../../../../typings/tsd.d.ts" />

class storageKeyProvider {

    static commonPrefix = "6.0";

    static storageKeyFor(value: string) {
        return storageKeyProvider.commonPrefix + value;
    }
    
}

export = storageKeyProvider;
