import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import messagePublisher = require("common/messagePublisher");

class createFolderInFilesystem extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    public folderName = ko.observable('');

    private folders : string[];

    constructor(folders) {
        super();
        this.folders = folders;
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();
        //this.folderName(true);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    create() {
        var folderName = this.folderName();

        if (this.isClientSideInputOK(folderName)) {
            this.creationTaskStarted = true;
            this.creationTask.resolve(folderName.toLowerCase());
            dialog.close(this);
        }
    }

    private isClientSideInputOK(folderName: string): boolean {
        var errorMessage;

        if (folderName == null) {
            errorMessage = "Please fill in the folder Name";
        }
        else if (this.folderExists(folderName, this.folders) === true) {
            errorMessage = "Folder already exists!";
        }

        if (errorMessage != null) {
            messagePublisher.reportError(errorMessage);
            return false;
        }
        return true;
    }

    private folderExists(folderName: string, folders: string[]): boolean {
        for (var i = 0; i < folders.length; i++) {
            if (folderName.toLowerCase() == folders[i]) {
                return true;
            }
        }
        return false;
    }

    enterKeyPressed() {
        var submit: boolean = this.folderName != null;

        if (this.folderName())
            submit = false;

        submit = this.folderName() && this.folderName().trim() != "";

        if (submit) {
            super.enterKeyPressed();
        }

        return submit;
    }
}

export = createFolderInFilesystem;