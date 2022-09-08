/// <reference path="../../../typings/tsd.d.ts" />

const versionFile = require("wwwroot/version.txt?raw");

class storageKeyProvider {

    static commonPrefix: string;

    static storageKeyFor(value: string) {
        storageKeyProvider.initCommonPrefix();
        return storageKeyProvider.commonPrefix + value;
    }
    
    private static initCommonPrefix(): void {
        if (storageKeyProvider.commonPrefix) {
            return;
        }
        
        try {
            const versionJson = JSON.parse(versionFile);
            const version = versionJson.Version.substring(0, 3);

            storageKeyProvider.commonPrefix = "ravendb-" + version + "-";
        } catch (e) {
            console.error("Unable to read version from version.txt file. Please make sure file doesn't contain BOM.");
            throw e;
        }
    }
}

export = storageKeyProvider;
