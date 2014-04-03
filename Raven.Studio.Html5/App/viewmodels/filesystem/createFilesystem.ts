import app = require("durandal/app");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import commandBase = require("commands/commandBase");

import filesystem = require("models/filesystem/filesystem");
import createFilesystemCommand = require("commands/filesystem/createFilesystemCommand");

class createFilesystem extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    public filesystemName = ko.observable('');
    public filesystemPath = ko.observable('');
    public filesystemLogs = ko.observable('');
    public filesystemNameFocus = ko.observable(true);

    private filesystems = ko.observableArray<filesystem>();
    private newCommandBase = new commandBase();

    constructor(filesystems) {
        super();
        this.filesystems = filesystems;
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();
        this.filesystemNameFocus(true);
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

        var filesystemName = this.filesystemName();

        if (this.isClientSideInputOK(filesystemName)) {
            this.creationTaskStarted = true;
            this.creationTask.resolve(filesystemName);
            dialog.close(this);
        }
    }

    private isClientSideInputOK(filesystemName): boolean {
        var errorMessage = "";

        if (filesystemName == null) {
            errorMessage = "Please fill out the Database Name field";
        }
        else if (this.isFilesystemNameExists(filesystemName, this.filesystems()) === true) {
            errorMessage = "Database Name Already Exists!";
        }
        else if ((errorMessage = this.CheckInput(filesystemName)) != null) { }

        if (errorMessage != null) {
            this.newCommandBase.reportError(errorMessage);
            this.filesystemNameFocus(true);
            return false;
        }
        return true;
    }

    private CheckInput(name): string {
        var rg1 = /^[^\\/:\*\?"<>\|]+$/; // forbidden characters \ / : * ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var maxLength = 260 - 30;

        var message = null;
        if (name.length > maxLength) {
            message = "The filesystem length can't exceed " + maxLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The filesystem name can't contain any of the following characters: \ / : * ?" + ' " ' + "< > |";
        }
        else if (rg2.test(name)) {
            message = "The filesystem name can't start with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        return message;       
    }

    private isFilesystemNameExists(filesystemName: string, filesystems: filesystem[]): boolean {
        for (var i = 0; i < filesystems.length; i++) {
            if (filesystemName == filesystems[i].name) {
                return true;
            }
        }
        return false;
    }
}

export = createFilesystem;