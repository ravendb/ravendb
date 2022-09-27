/// <reference path="../../../typings/tsd.d.ts" />

import versionProvider from "common/versionProvider";

class storageKeyProvider {

    static get commonPrefix() {
        return "ravendb-" + versionProvider.version + "-";
    }
    
    static storageKeyFor(value: string) {
        return storageKeyProvider.commonPrefix + value;
    }
}

export = storageKeyProvider;
