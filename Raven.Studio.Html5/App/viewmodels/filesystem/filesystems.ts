import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");

class filesystems extends viewModelBase {

    appUrls: computedAppUrls;
    fileSystems = ko.observableArray<filesystem>();
    isAnyFileSystemSelected: KnockoutComputed<boolean>;
    allCheckedFileSystemsDisabled: KnockoutComputed<boolean>;
    searchText = ko.observable("");
    selectedFileSystem = ko.observable<filesystem>();
    optionsClicked = ko.observable<boolean>(false);

    constructor() {
        super();

        this.fileSystems = shell.fileSystems;
        this.searchText.extend({ throttle: 200 }).subscribe(s => this.filterFilesystems(s));
        
        var currentFileSystem = this.activeFilesystem();
        if (!!currentFileSystem) {
            this.selectFileSystem(currentFileSystem, false);
        }

        var updatedUrl = appUrl.forFilesystems();
        this.updateUrl(updatedUrl);

        this.isAnyFileSystemSelected = ko.computed(() => {
            for (var i = 0; i < this.fileSystems().length; i++) {
                var fs: filesystem = this.fileSystems()[i];
                if (fs.isChecked()) {
                    return true;
                }
            }

            return false;
        });

        this.allCheckedFileSystemsDisabled = ko.computed(() => {
            var disabledStatus = null;
            for (var i = 0; i < this.fileSystems().length; i++) {
                var fs: filesystem = this.fileSystems()[i];
                if (fs.isChecked()) {
                    if (disabledStatus == null) {
                        disabledStatus = fs.disabled();
                    } else if (disabledStatus != fs.disabled()) {
                        return null;
                    }
                }
            }

            return disabledStatus;
        });
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
            this.newFileSystem();
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

        this.fileSystems().map((fs: filesystem) => fs.isChecked(!fs.isVisible() ? false : fs.isChecked()));
    }

    getFilesystemFilesUrl(fs: filesystem) {
        return appUrl.forFilesystemFiles(fs);
    }

    selectFileSystem(fs: filesystem, activateFileSystem: boolean = true) {
        if (this.optionsClicked() == false) {
            if (activateFileSystem) {
                fs.activate();
            }
            this.selectedFileSystem(fs);
        }

        this.optionsClicked(false);
    }

    newFileSystem() {
        require(["viewmodels/filesystem/createFilesystem"], createFileSystem => {
            var createFileSystemViewModel = new createFileSystem(this.fileSystems);
            createFileSystemViewModel
                .creationTask
                .done((fileSystemName: string, fileSystemPath: string, fileSystemLogsPath: string) => this.showCreationAdvancedStepsIfNecessary(fileSystemName, fileSystemPath, fileSystemLogsPath));
            app.showDialog(createFileSystemViewModel);
        });
    }

    showCreationAdvancedStepsIfNecessary(fileSystemName: string, fileSystemPath: string, fileSystemLogsPath: string) {
        require(["commands/filesystem/createFilesystemCommand"], createFileSystemCommand => {
            new createFileSystemCommand(fileSystemName, fileSystemPath).execute()
                .done(() => {
                    var newFileSystem = this.addNewFileSystem(fileSystemName);
                    this.selectFileSystem(newFileSystem);
                });
        });
    }

    private addNewFileSystem(fileSystemName: string): filesystem {
        var foundFileSystem = this.fileSystems.first((fs: filesystem) => fs.name == fileSystemName);

        if (!foundFileSystem) {
            var newFileSystem = new filesystem(fileSystemName);
            this.fileSystems.unshift(newFileSystem);
            return newFileSystem;
        }

        return foundFileSystem;
    }

    deleteSelectedFileSystems(fileSystems: Array<filesystem>) {
        if (fileSystems.length > 0) {
            require(["viewmodels/deleteResourceConfirm"], deleteResourceConfirm => {
                var confirmDeleteViewModel = new deleteResourceConfirm(fileSystems);

                confirmDeleteViewModel.deleteTask.done((deletedFileSystemsNames: string[]) => {
                    if (fileSystems.length == 1) {
                        this.onFileSystemDeleted(fileSystems[0].name);
                    } else {
                        deletedFileSystemsNames.forEach(fileSystemName => {
                            this.onFileSystemDeleted(fileSystemName);
                        });
                    }
                });

                app.showDialog(confirmDeleteViewModel);
            });
        }
    }

    deleteCheckedFileSystems() {
        var checkedFileSystems: filesystem[] = this.fileSystems().filter((fs: filesystem) => fs.isChecked());
        this.deleteSelectedFileSystems(checkedFileSystems);
    }

    private onFileSystemDeleted(fileSystemName: string) {
        var fileSystemInArray = this.fileSystems.first((fs: filesystem) => fs.name == fileSystemName);

        if (!!fileSystemInArray) {
            this.fileSystems.remove(fileSystemInArray);

            if ((this.fileSystems().length > 0) && (this.fileSystems.contains(this.selectedFileSystem()) === false)) {
                this.selectedFileSystem(this.fileSystems().first());
            }
        }
    }

    toggleSelectedFileSystems(fileSystems: Array<filesystem>) {
        if (fileSystems.length > 0) {
            var action = !fileSystems[0].disabled();

            require(["viewmodels/disableResourceToggleConfirm"], disableResourceToggleConfirm => {
                var disableResourceToggleViewModel = new disableResourceToggleConfirm(fileSystems);

                disableResourceToggleViewModel.disableToggleTask
                    .done((toggledFileSystemsNames: string[]) => {
                        var activeFileSystem: filesystem = this.activeFilesystem();

                        if (fileSystems.length == 1) {
                            this.onFileSystemDisabledToggle(fileSystems[0].name, action, activeFileSystem);
                        } else {
                            toggledFileSystemsNames.forEach(fileSystemName => {
                                this.onFileSystemDisabledToggle(fileSystemName, action, activeFileSystem);
                            });
                        }
                    });

                app.showDialog(disableResourceToggleViewModel);
            });
        }
    }

    toggleCheckedFileSystems() {
        var checkedFileSystems: filesystem[] = this.fileSystems().filter((fs: filesystem) => fs.isChecked());
        this.toggleSelectedFileSystems(checkedFileSystems);
    }

    private onFileSystemDisabledToggle(fileSystemName: string, action: boolean, activeFileSystem: filesystem) {
        var fs = this.fileSystems.first((foundFs: filesystem) => foundFs.name == fileSystemName);

        if (!!fs) {
            fs.disabled(action);
            fs.isChecked(false);

            if (!!activeFileSystem && fs.name == activeFileSystem.name) {
                this.selectFileSystem(fs);
            }
        }
    }
}

export = filesystems; 