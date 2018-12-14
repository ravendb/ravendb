import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import jsonUtil = require("common/jsonUtil");

class encryptionSettings {
    
    private encryptedDatabase = ko.observable<boolean>();
    private backupType: KnockoutObservable<Raven.Client.Documents.Operations.Backups.BackupType>;
    
    enabled = ko.observable<boolean>(false);
    
    mode = ko.observable<Raven.Client.Documents.Operations.Backups.EncryptionMode>();
    key = ko.observable<string>();
    keyConfirmation = ko.observable<boolean>(false);

    enableUnencryptedBackupForEncryptedDatabase: boolean;

    canProvideOwnKey: KnockoutComputed<boolean>;
    canUseDatabaseKey: KnockoutComputed<boolean>;
    showKeySourceDropdown: KnockoutComputed<boolean>;
    enableKeySourceDropdown: KnockoutComputed<boolean>;
    showProvidedKeySection: KnockoutComputed<boolean>;
    
    dirtyFlag: () => DirtyFlag;
    
    encryptionModes = [
        {
            label: "Encrypt using database encryption key",
            value: "UseDatabaseKey"
        },
        {
            label: "Provide your own encryption key",
            value: "UseProvidedKey"
        }
    ] as Array<valueAndLabelItem<Raven.Client.Documents.Operations.Backups.EncryptionMode, string>>;

    validationGroup: KnockoutValidationGroup;
    
    constructor(encryptedDatabase: boolean, enableUnencryptedBackupForEncryptedDatabase: boolean, 
                backupType: KnockoutObservable<Raven.Client.Documents.Operations.Backups.BackupType>, 
                dto: Raven.Client.Documents.Operations.Backups.EncryptionSettings) {
        this.encryptedDatabase(encryptedDatabase);
        this.backupType = backupType;
        this.enableUnencryptedBackupForEncryptedDatabase = enableUnencryptedBackupForEncryptedDatabase;
        
        this.enabled(encryptedDatabase);
        this.key(dto ? dto.Key : undefined);
        if (dto && dto.EncryptionMode) {
            this.mode(dto.EncryptionMode);
        } else {
            this.mode(backupType() === "Backup" ? "UseProvidedKey": "UseDatabaseKey");
        }
        
        this.initObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.mode,
            this.key
        ], false, jsonUtil.newLineNormalizingHashFunction);
        
        _.bindAll(this, "useEncryptionType");
    }
    
    private initObservables() {
        this.backupType.subscribe(backupType => {
            const dbIsEncrypted = this.encryptedDatabase();
            if (dbIsEncrypted) {
                if (this.backupType() === "Snapshot") {
                    this.mode("UseDatabaseKey");
                }
            } else {
                if (this.backupType() === "Backup") {
                    this.mode("UseProvidedKey");
                }
            }
        });
        
        this.key.subscribe(() => this.keyConfirmation(false));
        
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
            const encryptBackup = this.enabled();
            const canProvideKey = this.canProvideOwnKey();
            const canUseDbKey = this.canUseDatabaseKey();
            return encryptBackup && (canProvideKey || canUseDbKey);
        });
        
        this.enableKeySourceDropdown = ko.pureComputed(() => {
            const encryptBackup = this.enabled();
            const canProvideKey = this.canProvideOwnKey();
            const canUseDbKey = this.canUseDatabaseKey();
            return canProvideKey && canUseDbKey;
        });
        
        this.showProvidedKeySection = ko.pureComputed(() => {
            const encryptBackup = this.enabled();
            const type = this.backupType();
            const mode = this.mode();
            return encryptBackup && type === "Backup" && mode === "UseProvidedKey";
        });
        
        const self = this;
        this.enabled.extend({
            validation: [{
                validator: function(enabled: boolean) {
                    const dbIsEncrypted = self.encryptedDatabase();
                    if (dbIsEncrypted) {
                        if (!self.enabled()) {
                            switch (self.backupType()) {
                                case "Snapshot":
                                    this.message = "A 'Snapshot' backup-type was selected. An Unencrypted backup can only be defined for Encrypted databases when a 'Backup' backup-type is selected.";
                                    return false;
                                case "Backup":
                                    if (self.enableUnencryptedBackupForEncryptedDatabase) {
                                        return true;
                                    } else {
                                        this.message = "Your database is Encrypted. In order to create an Unencrypted backup, option 'Backup.EnableUnencryptedBackupForEncryptedDatabase' must be enabled by the system administrator. "; 
                                        return false;
                                    }
                            }
                        }
                    } else {
                        if (self.enabled() && self.backupType() === "Snapshot") {
                            this.message = "A 'Snapshot' backup-type was selected. Creating an Encrypted backup for Unencrypted databases is only supported when selecting the 'Backup' backup-type.";
                            return false;
                        }
                    }
                    
                    return true;
                }
            }]
        });

        this.key.extend({
            required: {
                onlyIf: () => this.canProvideOwnKey() && this.mode() === "UseProvidedKey"
            }
        });

        setupEncryptionKey.setupKeyValidation(this.key);
        setupEncryptionKey.setupConfirmationValidation(this.keyConfirmation);

        this.validationGroup = ko.validatedObservable({
            key: this.key,
            mode: this.mode,
            keyConfirmation: this.keyConfirmation
        });
    }

    labelFor(mode: Raven.Client.Documents.Operations.Backups.EncryptionMode) {
        const matched = this.encryptionModes.find(x => x.value === mode);
        return matched ? matched.label : null;
    }

    useEncryptionType(mode: Raven.Client.Documents.Operations.Backups.EncryptionMode) {
        this.mode(mode);
    }
    
    toDto(): Raven.Client.Documents.Operations.Backups.EncryptionSettings {
        return {
            EncryptionMode: this.mode(),
            Key: this.mode() === "UseProvidedKey" ? this.key() : undefined
        }
    }
    

}

export = encryptionSettings;
