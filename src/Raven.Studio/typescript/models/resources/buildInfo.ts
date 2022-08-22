/// <reference path="../../../typings/tsd.d.ts"/>

class buildInfo {
    static serverBuildVersion = ko.observable<serverBuildVersionDto>();
    
    static isDevVersion = ko.pureComputed(() => {
        const version = buildInfo.serverBuildVersion();
        return version && version.BuildVersion < 100;
    });

    /**
     * Example: 4.0, 4.2, 4.4, 5.2, 6.0
     */
    static mainVersion = ko.pureComputed(() => {
       const version = buildInfo.serverBuildVersion();
       return version ? version.FullVersion.substring(0, 3) : "n/a";
    });
    
    static onServerBuildVersion(dto: serverBuildVersionDto) {
        buildInfo.serverBuildVersion(dto);
    }
}

export = buildInfo;
