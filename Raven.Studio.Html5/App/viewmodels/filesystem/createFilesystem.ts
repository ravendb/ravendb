import app = require("durandal/app");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import filesystem = require("models/filesystem/filesystem");

class createFilesystem extends dialogViewModelBase {

    public creationTask = $.Deferred<fileSystemSettingsDto>();
    creationTaskStarted = false;

    public fileSystemName = ko.observable('');
    nameCustomValidityError: KnockoutComputed<string>;
    fileSystemPath = ko.observable('');
    pathCustomValidityError: KnockoutComputed<string>;
    fileSystemLogsPath = ko.observable('');
    storageEngine = ko.observable('');

    logsCustomValidityError: KnockoutComputed<string>;
    fileSystemNameFocus = ko.observable(true);

    constructor(private filesystems: KnockoutObservableArray<filesystem>) {
        super();

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newFileSystemName = this.fileSystemName();

            if (this.isFilesystemNameExists(newFileSystemName, this.filesystems()) == true) {
                errorMessage = "File system name already exists!";
            }
            else if ((errorMessage = this.checkName(newFileSystemName)) != '') { }

            return errorMessage;
        });

        this.pathCustomValidityError = ko.computed(() => {
            var newPath = this.fileSystemPath();
            var errorMessage: string = this.isPathLegal(newPath, "Path");
            return errorMessage;
        });

        this.logsCustomValidityError = ko.computed(() => {
            var newPath = this.fileSystemLogsPath();
            var errorMessage: string = this.isPathLegal(newPath, "Logs");
            return errorMessage;
        });
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();
        this.fileSystemNameFocus(true);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    nextOrCreate() {
        // For now we're just creating the filesystem.
        this.creationTaskStarted = true;
        dialog.close(this);
        this.creationTask.resolve({
            name: this.fileSystemName(),
            path: this.fileSystemPath(),
            logsPath: this.fileSystemLogsPath(),
            storageEngine: this.storageEngine()
        });
    }

    private isFilesystemNameExists(fileSystemName: string, filesystems: filesystem[]): boolean {
        fileSystemName = fileSystemName.toLowerCase();
        for (var i = 0; i < filesystems.length; i++) {
            if (fileSystemName == filesystems[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
    }

    private checkName(name: string): string {
        var rg1 = /^[^\\/:\*\?"<>\|]+$/; // forbidden characters \ / : * ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var maxLength = 260 - 30;

        var message = "";
        if (!$.trim(name)) {
            message = "Please fill out the file system name field!";
        }
        else if (name.length > maxLength) {
            message = "The file system length can't exceed " + maxLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The file system name can't contain any of the following characters: \ / : * ?" + ' " ' + "< > |";
        }
        else if (rg2.test(name)) {
            message = "The file system name can't start with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        return message;       
    }

    private isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^*\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = "";

        if (!$.trim(name) == false) { // if name isn't empty or not consist of only whitepaces
            if (name.length > 248) {
                errorMessage = "The path name for the '" + pathName + "' can't exceed " + 248 + " characters!";
            } else if (!rg1.test(name)) {
                errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                errorMessage = "The name '" + name + "' is forbidden for use!";
            }
        }
        return errorMessage;
    }
}

export = createFilesystem;