import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");

class defineNodeEncryptionKey extends dialogViewModelBase {

    key = ko.observable<string>();
    confirmation = ko.observable<boolean>(false);
    databaseName: string;
    nodeTag: string;
    
    encryptionSection: setupEncryptionKey;
    validationGroup: KnockoutValidationGroup;
    
    constructor(databaseName: string, nodeTag: string) {
        super();
        
        this.databaseName = databaseName;
        this.nodeTag = nodeTag;
        this.encryptionSection = new setupEncryptionKey(this.key,  this.confirmation, ko.observable(databaseName));
        
        this.initValidation();
    }
    
    private initValidation() {
        this.key.extend({
            required: true,
            base64: true
        });

        this.confirmation.extend({
            validation: [
                {
                    validator: (v: boolean) => v,
                    message: "Please confirm that you have saved the encryption key"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            key: this.key,
            confirmation: this.confirmation
        });
    }
    
    activate() {
        return this.encryptionSection.generateEncryptionKey();
    }
    
    compositionComplete() {
        super.compositionComplete();

        this.encryptionSection.syncQrCode();
        this.setupDisableReasons("#savingKeyData");
        
        this.key.subscribe(() => {
            this.encryptionSection.syncQrCode();
            // reset confirmation
            this.confirmation(false);
        });
    }
    
    addNode() {
        if (this.isValid(this.validationGroup)) {
            
            new distributeSecretCommand(this.databaseName, this.key(), [this.nodeTag])
                .execute()
                .done(() => {
                    dialog.close(this, true);
                })
                .fail(() => dialog.close(this));
        }
    }
}

export = defineNodeEncryptionKey;
