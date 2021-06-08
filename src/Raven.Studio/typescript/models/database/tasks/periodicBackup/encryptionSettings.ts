import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import jsonUtil = require("common/jsonUtil");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class encryptionSettings {
    
    private encryptedDatabase = ko.observable<boolean>();
    private backupType: KnockoutObservable<Raven.Client.Documents.Operations.Backups.BackupType>;
    
    encryptionSection: setupEncryptionKey;
    
    enabled = ko.observable<boolean>(false);
    mode = ko.observable<Raven.Client.Documents.Operations.Backups.EncryptionMode>();    
    originalKey = ko.observable<string>(); // current key of an existing encrypted backup
    key = ko.observable<string>();         // the new key that is selected in UI
    changeKeyRequest = ko.observable<boolean>(false);
    keyConfirmation = ko.observable<boolean>(false);    
    allowUnencryptedBackupForEncryptedDatabase = ko.observable<boolean>(false);

    canProvideOwnKey: KnockoutComputed<boolean>;
    canUseDatabaseKey: KnockoutComputed<boolean>;
    
    showKeySourceDropdown: KnockoutComputed<boolean>;
    enableKeySourceDropdown: KnockoutComputed<boolean>;
    keySourceDropdownTitle: KnockoutComputed<string>;

    showOriginalKeySection: KnockoutComputed<boolean>;
    showProvidedKeySection: KnockoutComputed<boolean>;  
    
    needExplicitConsent = ko.pureComputed(() => !this.enabled() && this.encryptedDatabase());
    
    dirtyFlag: () => DirtyFlag;
    
    encryptionModes = [
        {
            label: "Encrypt using Database Encryption Key",
            value: "UseDatabaseKey"
        },
        {
            label: "Provide your own Encryption Key",
            value: "UseProvidedKey"
        }
    ] as Array<valueAndLabelItem<Raven.Client.Documents.Operations.Backups.EncryptionMode, string>>;

    validationGroup: KnockoutComputed<KnockoutValidationGroup>;
    validationGroupWithKey: KnockoutValidationGroup;
    validationGroupWithoutKey: KnockoutValidationGroup;
    
    constructor(private databaseName: KnockoutObservable<string>,
        encryptedDatabase: boolean,
        backupType: KnockoutObservable<Raven.Client.Documents.Operations.Backups.BackupType>,
        dto: Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings,
        private isServerWideBackupTask: boolean = false) {
        
        this.encryptedDatabase(encryptedDatabase);
        this.backupType = backupType;
       
        // 'originalKey' will hold current key used by the backup (if exists)
        // 'key' will hold a new key when generated from the UI
        this.originalKey(dto && dto.EncryptionMode !== 'None' ? dto.Key : undefined);

        if (!dto) {
            if (encryptedDatabase) {
                // new one or wasn't set in the client API
                this.enabled(encryptedDatabase);
                this.mode("UseDatabaseKey");
            }
        }
        else if (dto.EncryptionMode) {
            this.mode(dto.EncryptionMode);

            if (dto.EncryptionMode === "None") {
                // it was already confirmed
                this.allowUnencryptedBackupForEncryptedDatabase(true);
            } else {
                this.enabled(true);
            }
        } else {
            this.mode(backupType() === "Backup" ? "UseProvidedKey": "UseDatabaseKey");
        }
        
        this.initObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.mode,
            this.allowUnencryptedBackupForEncryptedDatabase,
            this.changeKeyRequest
        ], false, jsonUtil.newLineNormalizingHashFunction);
        
        _.bindAll(this, "setEncryptionType");
    }
    
    private initObservables() {
        this.validationGroup = ko.pureComputed(() => {
            
            if (this.enabled() && this.mode() === 'UseDatabaseKey') {
                return this.validationGroupWithoutKey;
            }
            
            return this.enabled() ? this.validationGroupWithKey : this.validationGroupWithoutKey;
        });

        setupEncryptionKey.setupKeyValidation(this.key);
        
        this.backupType.subscribe(backupType => {
            const dbIsEncrypted = this.encryptedDatabase();
            if (dbIsEncrypted) {
                if (this.backupType() === "Snapshot") {
                    this.mode("UseDatabaseKey");
                }
            } else {
                // db not encrypted
                if (this.backupType() === "Backup" && this.enabled()) {
                    this.mode("UseProvidedKey");

                    if (!this.originalKey()) {
                        return this.encryptionSection.generateEncryptionKey();
                    }
                }
            }
        });
        
        this.mode.subscribe(mode => {
            if (this.encryptedDatabase() && mode === 'UseDatabaseKey') {
                this.changeKeyRequest(false); // imho - better to clear this checkbox
            }
            
            if (this.encryptedDatabase() && mode === 'UseProvidedKey' && !this.key()) {
                return this.encryptionSection.generateEncryptionKey();
            }
        });
        
        this.key.subscribe(() => {
            this.encryptionSection.syncQrCode();
            this.keyConfirmation(false);
        });
        
        this.encryptionSection = setupEncryptionKey.forBackup(this.key, this.keyConfirmation, this.databaseName);
        
        this.canProvideOwnKey = ko.pureComputed(() => {
            const type = this.backupType();
            const encryptBackup = this.enabled();
            return encryptBackup && type === "Backup";
        });
        
        this.canUseDatabaseKey = ko.pureComputed(() => {
            const isDbEncrypted = this.encryptedDatabase();
            const encryptBackup = this.enabled();
            return isDbEncrypted && encryptBackup;
        });
        
        this.showKeySourceDropdown = ko.pureComputed(() => {
            const encryptBackup = this.enabled() && this.enabled.isValid();
            const canProvideKey = this.canProvideOwnKey();
            const canUseDbKey = this.canUseDatabaseKey();
            return encryptBackup && (canProvideKey || canUseDbKey);
        });
        
        this.keySourceDropdownTitle = ko.pureComputed(() => {
            if (this.encryptedDatabase()) {
                return this.enableKeySourceDropdown() ? 'Select Encryption Key mode' :
                                                        'The Database Encryption Key will be used when selecting Snapshot type';
            }
            // db is not encrypted and the dropdown is disabled... 
            return 'The database is not encrypted. Provide your own Encryption Key for the Backup task.';
        });
        
        this.enableKeySourceDropdown = ko.pureComputed(() => {
            const canProvideKey = this.canProvideOwnKey();
            const canUseDbKey = this.canUseDatabaseKey();
            return canProvideKey && canUseDbKey;
        });
        
        this.showProvidedKeySection = ko.pureComputed(() => {
            const encryptBackup = this.enabled() && this.enabled.isValid();
            const type = this.backupType();
            const mode = this.mode();
            return encryptBackup && 
                   type === "Backup" && 
                   mode === "UseProvidedKey" &&
                   (!this.originalKey() || (this.originalKey() && this.changeKeyRequest()));
        });

        this.showOriginalKeySection = ko.pureComputed(() => {
            const canShowOriginalKey = this.enabled() && this.enabled.isValid() && 
                                       !!this.originalKey() && 
                                       this.mode() !== 'UseDatabaseKey';
            
            if (this.isServerWideBackupTask) {
                return canShowOriginalKey && this.backupType() !== "Snapshot";
            }
            
            return canShowOriginalKey;
        });
        
        this.allowUnencryptedBackupForEncryptedDatabase.extend({
            validation: [{
                validator: (v: boolean) => this.needExplicitConsent() ? v : true,
                message: "Please confirm you want to perform unencrypted backup of encrypted database"
            }]
        });

        const self = this;
        this.enabled.extend({
            validation: [{
                validator: function(enabled: boolean) {
                    if (self.enabled() &&  !self.backupType()) {
                        this.message = "Backup Type was not selected. Select Backup Type to continue.";
                        return false;
                    }
                    
                    const dbIsEncrypted = self.encryptedDatabase();
                    if (dbIsEncrypted) {
                        if (!self.enabled()) {
                            switch (self.backupType()) {
                                case "Snapshot":
                                    this.message = "A 'Snapshot' backup-type was selected. An Unencrypted backup can only be defined for Encrypted databases when a 'Backup' backup-type is selected.";
                                    return false;
                                case "Backup":
                                    return true;
                            }
                        }
                    } else {
                        if (self.enabled() && self.backupType() === "Snapshot" && !self.isServerWideBackupTask) {
                            this.message = "A 'Snapshot' backup-type was selected. Creating an Encrypted backup for Unencrypted databases is only supported when selecting the 'Backup' backup-type.";
                            return false;
                        }
                    }
                    return true;
                }
            }]
        });
        
        this.enabled.subscribe(enabled => {
            if (enabled && this.enabled.isValid()) {
                this.mode("UseProvidedKey");
                
                if (!this.originalKey()) {
                    return this.encryptionSection.generateEncryptionKey();
                }
            }
            
            if (!this.enabled()) {
                this.changeKeyRequest(false);
            }
        });

        this.changeKeyRequest.subscribe(changeKeyRequest => {
            if (changeKeyRequest) {
                return this.encryptionSection.generateEncryptionKey();
            }
        });

        const keyConfirmationNeeded = ko.pureComputed(() =>
            this.canProvideOwnKey() &&
            (!this.originalKey() || this.changeKeyRequest())
        );
        
        this.key.extend({
           required: {
               onlyIf: () => keyConfirmationNeeded()
           } 
        });
        
        this.keyConfirmation.extend({
            required: {
                onlyIf: () => keyConfirmationNeeded()
            }
        });
        
        setupEncryptionKey.setupConfirmationValidation(this.keyConfirmation, keyConfirmationNeeded);

        this.validationGroupWithKey = ko.validatedObservable({
            enabled: this.enabled,
            key: this.key,
            keyConfirmation: this.keyConfirmation, 
            allowUnencryptedBackupForEncryptedDatabase: this.allowUnencryptedBackupForEncryptedDatabase
        });
        
        this.validationGroupWithoutKey = ko.validatedObservable({
            enabled: this.enabled,
            allowUnencryptedBackupForEncryptedDatabase: this.allowUnencryptedBackupForEncryptedDatabase
        });
    }

    labelFor(mode: Raven.Client.Documents.Operations.Backups.EncryptionMode) {
        const matched = this.encryptionModes.find(x => x.value === mode);
        return matched ? matched.label : null;
    }

    setEncryptionType(mode: Raven.Client.Documents.Operations.Backups.EncryptionMode) {
        this.mode(mode);
    }

    setKeyUsedBeforeSave() {
        // Use 'original key' for 'save' when: 
        // opened existing encrypted backup and didn't request to change key
        // or if 'key' does exist but asked to change 
        // remember - 'key' has no value if wasn't generated from UI...        
        if (this.originalKey() && 
           (!this.key() || !this.changeKeyRequest())) {
            
            this.key(this.originalKey()); // using original
            this.keyConfirmation(true);   // needed for validation 
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings {
        return {
            EncryptionMode: this.enabled() ? this.mode() : "None",
            Key: this.mode() === "UseProvidedKey" ? this.key() : undefined
        }
    }
}

export = encryptionSettings;
