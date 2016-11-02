/// <reference path="../../../typings/tsd.d.ts"/>

class accessHelper {
    static isGlobalAdmin = ko.observable<boolean>(true); //TODO: set default to false
    static canReadWriteSettings = ko.observable<boolean>(false);
    static canReadSettings = ko.observable<boolean>(false);
    static canExposeConfigOverTheWire = ko.observable<boolean>(false);
}

export = accessHelper;