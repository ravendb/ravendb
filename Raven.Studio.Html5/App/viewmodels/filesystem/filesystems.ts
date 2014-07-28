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

    appUrls: computedAppUrls;
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

    activate(args) {
        super.activate(args);

        this.appUrls = appUrl.forCurrentFilesystem();
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
                .done((fileSystemName: string, fileSystemPath: string, fileSystemLogsPath: string) => this.showCreationAdvancedStepsIfNecessary(fileSystemName, fileSystemPath, fileSystemLogsPath));
            app.showDialog(createFilesystemViewModel);
        });
    }

    showCreationAdvancedStepsIfNecessary(fileSystemName: string, fileSystemPath: string, fileSystemLogsPath: string) {
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
}

export = filesystems; 