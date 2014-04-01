import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import commandBase = require("commands/commandBase");
import getNewEncryptionKey = require("commands/getNewEncryptionKey");
import checkEncryptionKey = require("commands/checkEncryptionKey");

class createEncryption extends dialogViewModelBase {

    public creationEncryption = $.Deferred();
    creationEncryptionStarted = false;

    key = ko.observable();
    encryptionAlgorithm = ko.observable('Rijndael');
    encryptionBits = ko.observable();
    isEncryptedIndexes = ko.observable(true);
    keyFocus = ko.observable(true);
    algorithmFocus = ko.observable(false);
    bitsFocus = ko.observable(false);

    private newCommandBase = new commandBase();
    private newEncryptionKey: getNewEncryptionKey;

    constructor() {
        super();

        this.newEncryptionKey = new getNewEncryptionKey();
        this.newEncryptionKey
            .execute()
            .done(result=> {
                this.key(result);
        });
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

    refresh_encryption() {
        this.newEncryptionKey
            .execute()
            .done(result=> {
                this.key(result);
            });
    }

    save() {
        var key = this.key();

        if (this.isClientSideInputOK(key)) {
            var checkEncryption: checkEncryptionKey = new checkEncryptionKey(key);
            checkEncryption
                .execute()
                .done(result=> {
                    if (Boolean(result)) {
                        this.creationEncryption.resolve(key, this.encryptionAlgorithm(), this.encryptionBits(), this.isEncryptedIndexes());
                        this.creationEncryptionStarted = true;
                        dialog.close(this);
                    }
                });
        }
    }

    private isClientSideInputOK(key): boolean {
        var base64Matcher = new RegExp("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})([=]{1,2})?$");
        var result = false;

        if (!key) {
            this.newCommandBase.reportError("Please fill out the key field");
            this.keyFocus(true);
        }
        else if (!base64Matcher.test(key.toString())) {
            this.newCommandBase.reportError("The key must be in base64 encoding format!");
            this.keyFocus(true);
        }
        else if (!this.encryptionAlgorithm()) {
            this.newCommandBase.reportError("Please select an encryption algorithm");
            this.algorithmFocus(true);
        }
        else if (!this.encryptionBits()) {
            this.newCommandBase.reportError("Please select an encryption key bits");
            this.bitsFocus(true);
        } else {
            result = true;
        }
        return result;
    }
}

export = createEncryption;