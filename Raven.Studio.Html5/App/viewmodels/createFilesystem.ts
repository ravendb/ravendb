import app = require("durandal/app");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import commandBase = require("commands/commandBase");

import filesystem = require("models/filesystem");
import createFilesystemCommand = require("commands/createFilesystemCommand");

class createFilesystem extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    filesystemName = ko.observable('');
    filesystemNameFocus = ko.observable(true);

    private filesystems = ko.observableArray<filesystem>();
    private newCommandBase = new commandBase();

    constructor(filesystems) {
        super();
        this.filesystems = filesystems;
    }

    cancel() {
        dialog.close(this);
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
        var result = false;
        var message = "";

        if (!filesystemName) {
            message = "Please fill out the file system name field";
        }
        else if (this.isFilesystemNameExists(filesystemName, this.filesystems()) == true) {
            message = "File system already exists!";
        }
        else if (!this.isValidName(filesystemName)) {
            message = "Please enter a valid file system name!";
        } else {
            result = true;
        }

        if (result == false) {
            this.newCommandBase.reportError(message);
            this.filesystemNameFocus(true);
        }

        return result;
    }

    private isValidName(name): boolean {
        var rg1 = /^[^\\/:\*\?"<>\|]+$/; // forbidden characters \ / : * ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        var maxLength;
        if (navigator.appVersion.indexOf("Win") != -1) {
            maxLength = 260;
        } else {
            maxLength = 255;
        }

        return rg1.test(name) && !rg2.test(name) && !rg3.test(name) && (name.length <= maxLength);
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