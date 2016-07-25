import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getNewEncryptionKey = require("commands/database/studio/getNewEncryptionKey");
import checkEncryptionKey = require("commands/database/studio/checkEncryptionKey");

class createEncryption extends dialogViewModelBase {

    public creationEncryption = $.Deferred();
    creationEncryptionStarted = false;

    key = ko.observable<string>();
    encryptionAlgorithm = ko.observable('Rijndael');
    encryptionBits = ko.observable<number>(256);
    isEncryptedIndexes = ko.observable(true);    

    private newEncryptionKey: getNewEncryptionKey;
    private base64Matcher = new RegExp("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})([=]{1,2})?$");

    constructor() {
        super();
        
        this.newEncryptionKey = new getNewEncryptionKey();
        this.newEncryptionKey
            .execute()
            .done(result=> {
                this.key(result);
            });
    }

    attached() {
        this.dialogSelectorName = "#CreateEncriptionDialog";
        super.attached();
        var inputElement: any = $("#key")[0];
        this.key.subscribe((newKey) => {
            if (!this.base64Matcher.test(newKey.toString())) {
                inputElement.setCustomValidity("The key must be in Base64 encoding format!");
            }
            else {
                inputElement.setCustomValidity('');
            }
        });
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationEncryptionStarted) {
            this.creationEncryption.reject();
        }
    }

    cancel() {
        dialog.close(this);
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

        var checkEncryption: checkEncryptionKey = new checkEncryptionKey(key);
        checkEncryption
            .execute()
            .done(()=> {
                this.creationEncryption.resolve(key, this.encryptionAlgorithm(), this.encryptionBits(), this.isEncryptedIndexes());
                this.creationEncryptionStarted = true;
                dialog.close(this);
            });
    }
}

export = createEncryption;
