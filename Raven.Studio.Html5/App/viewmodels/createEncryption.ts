import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createEncryption extends dialogViewModelBase {

    public creationEncryption = $.Deferred();
    creationEncryptionStarted = false;

    key = ko.observable();
    encryptionAlgorithm = ko.observable();
    isEncryptedIndexes = ko.observable(true);

    constructor() {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationEncryptionStarted) {
            this.creationEncryption.reject();
        }
    }

    save() {
        var key = this.key();
        var base64Matcher = new RegExp("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})([=]{1,2})?$");
        if (!base64Matcher.test(key.toString())) {
            // It's definitely not base64 encoded.
            // TODO: add format error for safari browser
        }
        else if (this.encryptionAlgorithm() === undefined) {
            // The encryption algorithm is undefined
            // TODO: add format error for safari browser
        } else {
            this.creationEncryption.resolve(key, this.encryptionAlgorithm(), this.isEncryptedIndexes());
            this.creationEncryptionStarted = true;
            dialog.close(this);
        }
    }

}

export = createEncryption;