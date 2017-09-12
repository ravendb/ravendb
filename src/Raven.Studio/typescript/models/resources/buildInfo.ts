/// <reference path="../../../typings/tsd.d.ts"/>

class buildInfo {
    static serverBuildVersion = ko.observable<serverBuildVersionDto>();
    static serverMainVersion = ko.observable<number>(4);
    static serverMinorVersion = ko.observable<number>(0);
}

export = buildInfo;
