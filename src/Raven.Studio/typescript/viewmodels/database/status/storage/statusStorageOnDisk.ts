import viewModelBase = require("viewmodels/viewModelBase");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import getStatusStorageOnDiskCommand = require("commands/database/debug/getStatusStorageOnDiskCommand");

class statusStorageOnDisk extends viewModelBase {
    onDiskStats = ko.observable<statusStorageOnDiskDto>();
    config = ko.observable<any>();
    isConfigForbidden: KnockoutComputed<boolean>;
    journalsStoragePath: KnockoutComputed<string>;

    constructor() {
        super();
        this.isConfigForbidden = ko.computed(() => !this.config());
        this.journalsStoragePath = ko.computed(() => {
            if (!!this.config() && !!this.config().JournalsStoragePath) {
                return this.config().JournalsStoragePath;
            }
            return "-";
        });
    }

    activate(args: any) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchData());
        return this.fetchData();
    }

    fetchData(): JQueryPromise<any> {
        var db = this.activeDatabase();
        if (db) {
            var onDiskTask = new getStatusStorageOnDiskCommand(db)
                .execute();
            var configTask = new getStatusDebugConfigCommand(db)
                .execute();

            var combinedTask = $.when(onDiskTask, configTask);
            combinedTask.done((onDisk, config) => {
                this.onDiskStats(onDisk[0]);
                this.config(config[0]);
            });
            return combinedTask;
        }

        return null;
    }
}

export = statusStorageOnDisk;
