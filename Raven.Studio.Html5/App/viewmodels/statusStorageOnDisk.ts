import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import getStatusDebugConfigCommand = require("commands/getStatusDebugConfigCommand");
import getStatusStorageOnDiskCommand = require("commands/getStatusStorageOnDiskCommand");

class statusStorageOnDisk extends viewModelBase {
    onDiskStats = ko.observable<statusStorageOnDiskDto>();
    config = ko.observable<any>();

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchData());
        return this.fetchData();
    }

    fetchData(): JQueryPromise<any> {
        var db = this.activeDatabase();
        if (db) {
            var configTask = new getStatusDebugConfigCommand(db)
                .execute();
            var onDiskTask = new getStatusStorageOnDiskCommand(db)
                .execute();

            var combinedTask = $.when(configTask, onDiskTask);
            combinedTask.done((config, onDisk) => {
                this.config(config[0]);
                this.onDiskStats(onDisk[0]);
            });
            return combinedTask;
        }

        return null;
    }
}

export = statusStorageOnDisk;