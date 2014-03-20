import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createEncryptionConfirmation extends dialogViewModelBase {

    key = ko.observable();

    private copystr;

    constructor(savedKey) {
        super();
        this.key(savedKey);
        var isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
        if (isMac) {
            this.copystr = "Command";
        } else {
            this.copystr = "Ctrl";
        }
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

    copyToClipboard() {

        window.prompt("Copy to clipboard: " + this.copystr + "+C, Enter", this.key().toString());
    }

}

export = createEncryptionConfirmation;
