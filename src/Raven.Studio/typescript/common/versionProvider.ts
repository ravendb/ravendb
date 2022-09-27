/// <reference path="../../typings/tsd.d.ts" />

const versionFile = require("wwwroot/version.txt?raw");

class versionProvider {

    private static versionCached: string;

    static get version(): string {
        versionProvider.initVersion();
        
        return versionProvider.versionCached;
    }
    
    private static initVersion(): void {
        if (versionProvider.versionCached) {
            return;
        }

        try {
            const versionJson = JSON.parse(versionFile);
            versionProvider.versionCached = versionJson.Version.substring(0, 3);
        } catch (e) {
            console.error("Unable to read version from version.txt file. Please make sure file doesn't contain BOM.");
            throw e;
        }
    }
}

export = versionProvider;
