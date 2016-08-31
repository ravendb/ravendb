/// <reference path="../../typings/tsd.d.ts"/>

import accessHelper = require("viewmodels/shell/accessHelper");

class settingsAccessAuthorizer {

    static isForbidden: KnockoutComputed<boolean> = ko.computed(() => {
        var globalAdmin = accessHelper.isGlobalAdmin();
        var canReadWriteSettings = accessHelper.canReadWriteSettings();
        var canReadSettings = accessHelper.canReadSettings();
        return !globalAdmin && !canReadWriteSettings && !canReadSettings;
    });

    static isReadOnly: KnockoutComputed<boolean> = ko.computed(() => {
        var globalAdmin = accessHelper.isGlobalAdmin();
        var canReadWriteSettings = accessHelper.canReadWriteSettings();
        var canReadSettings = accessHelper.canReadSettings();
        return !globalAdmin && !canReadWriteSettings && canReadSettings;
    });

    static canWrite: KnockoutComputed<boolean> = ko.computed(() => {
        var globalAdmin = accessHelper.isGlobalAdmin();
        var canReadWriteSettings = accessHelper.canReadWriteSettings();
        var canReadSettings = accessHelper.canReadSettings();
        return globalAdmin || canReadWriteSettings || canReadSettings;
    });

    static canReadOrWrite: KnockoutComputed<boolean> = ko.computed(() => {
        var globalAdmin = accessHelper.isGlobalAdmin();
        var canReadWriteSettings = accessHelper.canReadWriteSettings();
        return globalAdmin || canReadWriteSettings;
    });
}

export = settingsAccessAuthorizer;
