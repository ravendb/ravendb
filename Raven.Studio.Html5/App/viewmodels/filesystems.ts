import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import database = require("models/database");
import databases = require("viewmodels/databases");
import filesystem = require("models/filesystem");
import getFilesystemsCommand = require("commands/getFilesystemsCommand");
import getFilesystemStatsCommand = require("commands/getFilesystemStatsCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystems extends viewModelBase {

    filesystems = ko.observableArray<filesystem>();    
    searchText = ko.observable("");
    selectedFilesystem = ko.observable<filesystem>();
    defaultFs: filesystem;
    initializedStats: boolean;

    constructor() {
        super();

        this.defaultFs = appUrl.getDefaultFilesystem();
        this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterFilesystems(s));
    }

    modelPolling() {
        new getFilesystemsCommand()
            .execute()
            .done((results: filesystem[]) => this.filesystemsLoaded(results));
    }

    filterFilesystems(filter: string) {
        var filterLower = filter.toLowerCase();
        this.filesystems().forEach(d=> {
            var isMatch = !filter || (d.name.toLowerCase().indexOf(filterLower) >= 0);
            d.isVisible(isMatch);
        });
    }

    navigateToDocuments(fs: filesystem) {
        fs.activate();
        router.navigate(appUrl.forFilesystem(fs));
    }

    getFilesystemUrl(fs: filesystem) {
        return appUrl.forFilesystem(fs);
    }

    filesystemsLoaded(results: Array<filesystem>) {
        var filesystemsHaveChanged = this.checkDifferentFilesystems(results);
        if (filesystemsHaveChanged) {            
            this.filesystems(results);

            // If we have just a few filesystems, grab the fs stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            if (results.length < few && !this.initializedStats) {
                this.initializedStats = true;
                results.forEach(fs => this.fetchStats(fs));
            }
        }
    }

    checkDifferentFilesystems(fss: filesystem[]) {
        if (fss.length !== this.filesystems().length) {
            return true;
        }

        var freshFsNames = fss.map(fs => fs.name);
        var existingFsNames = this.filesystems().map(fs => fs.name);
        return existingFsNames.some(existing => !freshFsNames.contains(existing));
    }

    fetchStats(fs: filesystem) {
        new getFilesystemStatsCommand(fs)
            .execute()
            .done(result => fs.statistics(result));
    }

    selectFilesystem(fs: filesystem) {
        this.filesystems().forEach(d=> d.isSelected(d === fs));
        fs.activate();
        this.selectedFilesystem(fs);
    }

    // Federico: If we ever implement delete filesystems (which I believe it could be needed) uncomment and implement the deleteFilesystemConfig dialog.

    //deleteSelectedFilesystem() {
    //    var fs = this.selectedFilesystem();
    //    if (fs) {
    //        require(["viewmodels/deleteFilesystemConfirm"], deleteFilesystemConfirm => {
    //            var confirmDeleteVm: deleteFilesystemConfirm = new deleteFilesystemConfirm(fs, appUrl.getSystemDatabase());
    //            confirmDeleteVm.deleteTask.done(() => this.onFilesystemDeleted(fs));
    //            app.showDialog(confirmDeleteVm);
    //        });
    //    }
    //}

    //onFilesystemDeleted(fs: filesystem) {
    //    this.filesystems.remove(fs);
    //    if (this.selectedFilesystem() === fs) {
    //        this.selectedFilesystem(this.filesystems().first());
    //    }
    //}

}

export = filesystems; 