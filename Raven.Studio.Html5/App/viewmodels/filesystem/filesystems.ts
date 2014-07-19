import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import getFilesystemsCommand = require("commands/filesystem/getFilesystemsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import createFilesystem = require("viewmodels/filesystem/createFilesystem");
import createFilesystemCommand = require("commands/filesystem/createFilesystemCommand");


class filesystems extends viewModelBase {

    fileSystems = ko.observableArray<filesystem>();    
    searchText = ko.observable("");
    selectedFilesystem = ko.observable<filesystem>();

    constructor() {
        super();

        this.fileSystems = shell.fileSystems;
        this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterFilesystems(s));
        
        var currentFileSystem = this.activeFilesystem();
        if (!!currentFileSystem) {
            this.selectFileSystem(currentFileSystem, false);
        }
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any) {
        return true;
    }

    attached() {
        this.fileSystemsLoaded();
    }

    private fileSystemsLoaded() {
        // If we have no file systems, show the "create a new file system" screen.
        if (this.fileSystems().length === 0) {
            this.newFilesystem();
        } else {
            // If we have just a few file systems, grab the fs stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            var enabledFileSystems: filesystem[] = this.fileSystems().filter((fs: filesystem) => !fs.disabled());
            if (enabledFileSystems.length < few) {
                enabledFileSystems.forEach(fs => shell.fetchFsStats(fs));
            }
        }
    }

    filterFilesystems(filter: string) {
        var filterLower = filter.toLowerCase();
        this.fileSystems().forEach(d=> {
            var isMatch = !filter || (d.name.toLowerCase().indexOf(filterLower) >= 0);
            d.isVisible(isMatch);
        });
    }

    getFilesystemFilesUrl(fs: filesystem) {
        return appUrl.forFilesystemFiles(fs);
    }

    selectFileSystem(fs: filesystem, activateFileSystem: boolean = true) {
        this.fileSystems().forEach((f: filesystem) => f.isSelected(f.name === fs.name));
        if (activateFileSystem) {
            fs.activate();
        }
        this.selectedFilesystem(fs);
    }

    newFilesystem() {
        require(["viewmodels/filesystem/createFilesystem"], createFilesystem => {
            var createFilesystemViewModel: createFilesystem = new createFilesystem(this.fileSystems);
            createFilesystemViewModel
                .creationTask
                .done((filesystemName: string, filesystemPath: string) => this.showCreationAdvancedStepsIfNecessary(filesystemName, filesystemPath));
            app.showDialog(createFilesystemViewModel);
        });
    }

    showCreationAdvancedStepsIfNecessary(fileSystemName: string, fileSystemPath: string) {
        new createFilesystemCommand(fileSystemName, fileSystemPath).execute()
            .done(() => {
                var newFileSystem = this.addNewFileSystem(fileSystemName);
                this.selectFileSystem(newFileSystem);
        });
    }

    private addNewFileSystem(fileSystemName: string): filesystem {
        var fileSystemInArray = this.fileSystems.first((fs: filesystem) => fs.name == fileSystemName);

        if (!fileSystemInArray) {
            var newFileSystem = new filesystem(fileSystemName);
            this.fileSystems.unshift(newFileSystem);
            return newFileSystem;
        }

        return fileSystemInArray;
    }

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