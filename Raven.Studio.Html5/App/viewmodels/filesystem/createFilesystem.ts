import app = require("durandal/app");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import filesystem = require("models/filesystem/filesystem");

class createFilesystem extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;
    private emptyNameMessage = "Please fill out the file system name field!";

    public fileSystemName = ko.observable('');
    nameCustomValidity = ko.observable<string>(this.emptyNameMessage);
    public fileSystemPath = ko.observable('');
    pathCustomValidity = ko.observable<string>('');
    public fileSystemLogsPath = ko.observable('');
    logsCustomValidity = ko.observable<string>('');
    public fileSystemNameFocus = ko.observable(true);

    constructor(private filesystems: KnockoutObservableArray<filesystem>) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();

        this.fileSystemName.subscribe((newFileSystemName) => {
            var errorMessage: string = '';
            if (this.isFilesystemNameExists(newFileSystemName, this.filesystems()) === true) {
                errorMessage = "File system name already exists!";
            }
            else if ((errorMessage = this.checkName(newFileSystemName)) != '') { }

            this.nameCustomValidity(errorMessage);
        });

        this.subscribeToPath(this.fileSystemPath, this.pathCustomValidity, "Path");
        this.subscribeToPath(this.fileSystemLogsPath, this.logsCustomValidity, "Logs");

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
        this.creationTask.resolve(this.fileSystemName(), this.fileSystemPath(), this.fileSystemLogsPath());
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

        var message = '';
        if (!$.trim(name)) {
            message = this.emptyNameMessage;
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

    private subscribeToPath(element, validityObservable: KnockoutObservable<string>, pathName) {
        element.subscribe((path) => {
            var errorMessage: string = this.isPathLegal(path, pathName);
            validityObservable(errorMessage);
        });
    }

    private isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^*\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = null;

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