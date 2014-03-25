import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem");
import getFilesystemsCommand = require("commands/getFilesystemsCommand");
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
        //this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterDatabases(s));
    }

    modelPolling() {
        new getFilesystemsCommand()
            .execute()
            .done((results: filesystem[]) => this.filesystemsLoaded(results));
    }

    //navigateToDocuments(db: database) {
    //    db.activate();
    //    router.navigate(appUrl.forDocuments(null, db));
    //}

    //getDocumentsUrl(db: database) {
    //    return appUrl.forDocuments(null, db);
    //}

    filesystemsLoaded(results: Array<filesystem>) {
        var filesystemsHaveChanged = this.checkDifferentFilesystems(results);
        if (filesystemsHaveChanged) {            
            this.filesystems(results);

            // If we have just a few filesystems, grab the fs stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            if (results.length < few && !this.initializedStats) {
                this.initializedStats = true;
                //results.forEach(fs => this.fetchStats(fs));
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

    //fetchStats(fs: filesystem) {
    //    new getFilesystemStatsCommand(fs)
    //        .execute()
    //        .done(result => fs.statistics(result));
    //}
}

export = filesystems; 