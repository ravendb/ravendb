import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createEncryptionConfirmation extends dialogViewModelBase {

    message = ko.observable();

    constructor(message) {
        super();
        this.message(message);
    }

    cancel() {
        dialog.close(this);
    }

    ok() {
        dialog.close(this);
    }
}

export = createEncryptionConfirmation;
