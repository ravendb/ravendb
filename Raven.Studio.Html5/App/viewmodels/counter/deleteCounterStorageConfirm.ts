import deleteCounterStorageCommand = require("commands/counter/deleteCounterStorageCommand");
import counterStorage = require("models/counter/counterStorage");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteCounterStorageConfirm extends dialogViewModelBase {
    private isKeepingFiles = ko.observable(true);
    public deleteTask = $.Deferred();

    constructor(private storageToDelete: counterStorage) {
        super();

        if (!storageToDelete) {
            throw new Error("Must specified counter storage to delete.");
        }
    }
    
    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    deleteCounterStorage() {
        new deleteCounterStorageCommand(this.storageToDelete.name, this.isKeepingFiles() === false)
            .execute()
            .done(results => this.deleteTask.resolve(results))
            .fail(details => this.deleteTask.reject(details));
        
        dialog.close(this);
    }

    cancel() {
        this.deleteTask.reject();
        dialog.close(this);
    }
}

export = deleteCounterStorageConfirm;