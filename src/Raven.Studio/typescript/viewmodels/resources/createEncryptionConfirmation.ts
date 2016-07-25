import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createEncryptionConfirmation extends dialogViewModelBase {

    public dialogPromise = $.Deferred();

    key = ko.observable<string>();

    constructor(savedKey: string) {
        super();
        this.key(savedKey);
    }

    cancel() {
        dialog.close(this);
    }

    ok() {
        dialog.close(this);
    }

    deactivate() {
        this.dialogPromise.resolve();
    }

    clickKey() {
        $('#key').select();
    }
}

export = createEncryptionConfirmation;
