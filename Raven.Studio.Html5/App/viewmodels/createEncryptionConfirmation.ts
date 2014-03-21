import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createEncryptionConfirmation extends dialogViewModelBase {

    key = ko.observable();

    constructor(savedKey) {
        super();
        this.key(savedKey);
    }

    cancel() {
        dialog.close(this);
    }

    ok() {
        dialog.close(this);
    }

    clickKey() {
        $('#key').select();
    }
}

export = createEncryptionConfirmation;
