import deleteResourceCommand = require("commands/resources/deleteResourceCommand");
import resource = require("models/resources/resource");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import resourceInfo = require("models/resources/info/resourceInfo");

class deleteResourceConfirm extends dialogViewModelBase {
    private isKeepingFiles = ko.observable<boolean>(true);

    deleteTask = $.Deferred<Array<resource>>();
    isDeletingDatabase: boolean;

    constructor(private resourcesToDelete: Array<resourceInfo>) {
        super();
    }

    keepFiles() {
        this.isKeepingFiles(true);
    }

    deleteEverything() {
        this.isKeepingFiles(false);
    }

    deleteDatabase() {
        new deleteResourceCommand(this.resourcesToDelete.map(x => x.asResource()), !this.isKeepingFiles())
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

export = deleteResourceConfirm;
