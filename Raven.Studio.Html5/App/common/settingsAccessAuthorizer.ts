import shell = require("viewmodels/shell");

class settingsAccessAuthorizer {
    isForbidden: KnockoutComputed<boolean>;
    isReadOnly: KnockoutComputed<boolean>;
    canWrite: KnockoutComputed<boolean>;
    canReadOrWrite: KnockoutComputed<boolean>;

    constructor() {
        this.isForbidden = ko.computed(() => {
            var globalAdmin = shell.isGlobalAdmin();
            var canReadWriteSettings = shell.canReadWriteSettings();
            var canReadSettings = shell.canReadSettings();
            return !globalAdmin && !canReadWriteSettings && !canReadSettings;
        });
        this.isReadOnly = ko.computed(() => {
            var globalAdmin = shell.isGlobalAdmin();
            var canReadWriteSettings = shell.canReadWriteSettings();
            var canReadSettings = shell.canReadSettings();
            return !globalAdmin && !canReadWriteSettings && canReadSettings;
        });
        this.canReadOrWrite = ko.computed(() => {
            var globalAdmin = shell.isGlobalAdmin();
            var canReadWriteSettings = shell.canReadWriteSettings();
            var canReadSettings = shell.canReadSettings();
            return globalAdmin || canReadWriteSettings || canReadSettings;
        });
        this.canWrite = ko.computed(() => {
            var globalAdmin = shell.isGlobalAdmin();
            var canReadWriteSettings = shell.canReadWriteSettings();
            return globalAdmin || canReadWriteSettings;
        });
    }
}

export = settingsAccessAuthorizer;
