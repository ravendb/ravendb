/// <reference path="../../../../typings/tsd.d.ts"/>

import configuration = require("configuration");
import clusterNode = require("models/database/cluster/clusterNode");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");

class databaseCreationModel {
    
    readonly configurationSections: Array<availableConfigurationSection> = [
        {
            name: "Data source",
            id: "legacyMigration",
            alwaysEnabled: true,
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Backup source",
            id: "restore",
            alwaysEnabled: true,
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Encryption",
            id: "encryption",
            alwaysEnabled: false,
            enabled: ko.observable<boolean>(false)
        },
        {
            name: "Replication",
            id: "replication",
            alwaysEnabled: true,
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Path",
            id: "path",
            alwaysEnabled: true,
            enabled: ko.observable<boolean>(true)
        }
    ];

    spinners = {
        fetchingRestorePoints: ko.observable<boolean>(false)
    };

    name = ko.observable<string>("");

    creationMode: dbCreationMode = null;
    isFromBackupOrFromOfflineMigration: boolean;

    restore = {
        backupDirectory: ko.observable<string>(),
        backupDirectoryError: ko.observable<string>(null),
        lastFailedBackupDirectory: null as string,
        selectedRestorePoint: ko.observable<string>(),
        restorePoints: ko.observableArray<Raven.Server.Documents.PeriodicBackup.RestorePoint>([]),
        backupLocation: ko.observable<string>(),
        lastFileNameToRestore: ko.observable<string>(),
        isFocusOnBackupDirectory: ko.observable<boolean>()
    };
    
    restoreValidationGroup = ko.validatedObservable({ 
        selectedRestorePoint: this.restore.selectedRestorePoint,
        backupDirectory: this.restore.backupDirectory
    });
    
    legacyMigration = {
        showAdvanced: ko.observable<boolean>(false),
        
        isEncrypted: ko.observable<boolean>(false),
        isCompressed: ko.observable<boolean>(false),

        dataDirectory: ko.observable<string>(),
        dataExporterFullPath: ko.observable<string>(),
        
        batchSize: ko.observable<number>(),
        sourceType: ko.observable<legacySourceType>(),
        journalsPath: ko.observable<string>(),
        encryptionKey: ko.observable<string>(),
        encryptionAlgorithm: ko.observable<string>(),
        encryptionKeyBitsSize: ko.observable<number>()
    };
    
    legacyMigrationValidationGroup = ko.validatedObservable({
        dataDirectory: this.legacyMigration.dataDirectory,
        dataExporterFullPath: this.legacyMigration.dataExporterFullPath,
        sourceType: this.legacyMigration.sourceType,
        journalsPath: this.legacyMigration.journalsPath,
        encryptionKey: this.legacyMigration.encryptionKey,
        encryptionAlgorithm: this.legacyMigration.encryptionAlgorithm,
        encryptionKeyBitsSize: this.legacyMigration.encryptionKeyBitsSize
    });
    
    replication = {
        replicationFactor: ko.observable<number>(2),
        manualMode: ko.observable<boolean>(false),
        dynamicMode: ko.observable<boolean>(true),
        nodes: ko.observableArray<clusterNode>([])
    };

    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replication.replicationFactor,
        nodes: this.replication.nodes
    });

    path = {
        dataPath: ko.observable<string>(),
    };

    pathValidationGroup = ko.validatedObservable({
        dataPath: this.path.dataPath,
    });

    encryption = {
        key: ko.observable<string>(),
        confirmation: ko.observable<boolean>(false)
    };
   
    encryptionValidationGroup = ko.validatedObservable({
        key: this.encryption.key,
        confirmation: this.encryption.confirmation
    });

    globalValidationGroup = ko.validatedObservable({
        name: this.name,
    });

    constructor(mode: dbCreationMode) {
        this.creationMode = mode;
        this.isFromBackupOrFromOfflineMigration = mode !== "newDatabase";
        
        const legacyMigrationConfig = this.configurationSections.find(x => x.id === "legacyMigration");
        legacyMigrationConfig.validationGroup = this.legacyMigrationValidationGroup;
        
        const restoreConfig = this.configurationSections.find(x => x.id === "restore");
        restoreConfig.validationGroup = this.restoreValidationGroup;
        
        const encryptionConfig = this.getEncryptionConfigSection();
        encryptionConfig.validationGroup = this.encryptionValidationGroup;

        const replicationConfig = this.configurationSections.find(x => x.id === "replication");
        replicationConfig.validationGroup = this.replicationValidationGroup;

        const pathConfig = this.configurationSections.find(x => x.id === "path");
        pathConfig.validationGroup = this.pathValidationGroup;

        encryptionConfig.enabled.subscribe(() => {
           this.replication.replicationFactor(this.replication.nodes().length); 
        });
        
        this.replication.nodes.subscribe(nodes => {
            this.replication.replicationFactor(nodes.length);
        });

        this.replication.replicationFactor.subscribe(factor => {
            if (factor === 1) {
                this.replication.dynamicMode(false);
            }
        });

        let isFirst = true;
        this.restore.isFocusOnBackupDirectory.subscribe(hasFocus => {
            if (isFirst) {
                isFirst = false;
                return;
            }

            if (this.creationMode !== "restore")
                return;

            if (hasFocus)
                return;

            if (!this.restore.backupDirectory.isValid() &&
                this.restore.backupDirectory() === this.restore.lastFailedBackupDirectory)
                return;

            if (!this.restore.backupDirectory())
                return;

            this.spinners.fetchingRestorePoints(true);
            new getRestorePointsCommand(this.restore.backupDirectory())
                .execute()
                .done((restorePoints: Raven.Server.Documents.PeriodicBackup.RestorePoints) => {
                    this.restore.restorePoints(restorePoints.List.map(x => {
                        const date = x.Key;
                        const dateFormat = "YYYY MMMM Do, h:mm A";
                        x.Key = moment.utc(date).local().format(dateFormat);
                        return x;
                    }));
                    this.restore.selectedRestorePoint(null);
                    this.restore.backupLocation(null);
                    this.restore.lastFileNameToRestore(null);
                    this.restore.backupDirectoryError(null);
                    this.restore.lastFailedBackupDirectory = null;
                })
                .fail((response: JQueryXHR) => {
                    const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                    this.restore.backupDirectoryError(generalUtils.trimMessage(messageAndOptionalException.message));
                    this.restore.lastFailedBackupDirectory = this.restore.backupDirectory();
                    this.restore.backupDirectory.valueHasMutated();
                })
                .always(() => this.spinners.fetchingRestorePoints(false));
        });
        
        _.bindAll(this, "useRestorePoint");
    }

    getEncryptionConfigSection() {
        return this.configurationSections.find(x => x.id === "encryption");
    }

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>\|]*$/; // forbidden characters * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        observable.extend({
            maxLength: {
                params: maxLength,
                message: `Path name for '${name}' can't exceed ${maxLength} characters!`
            },
            validation: [{
                validator: (val: string) => rg1.test(val),
                message: `{0} path can't contain any of the following characters: * ? " < > |`,
                params: name
            },
            {
                validator: (val: string) => !rg3.test(val),
                message: `The name {0} is forbidden for use!`,
                params: this.name
            }]
        });
    }

    setupValidation(databaseDoesntExist: (name: string) => boolean, maxReplicationFactor: number) {
        this.setupPathValidation(this.path.dataPath, "Data");

        this.name.extend({
            required: true,
            validDatabaseName: true,

            validation: [
                {
                    validator: () => databaseDoesntExist,
                    message: "Database already exists"
                }
            ]
        });
        
        this.setupReplicationValidation(maxReplicationFactor);
        this.setupRestoreValidation();
        this.setupEncryptionValidation();
        this.setupLegacyMigrationValidation();
    }
    
    private setupReplicationValidation(maxReplicationFactor: number) {
        this.replication.nodes.extend({
            validation: [{
                validator: (val: Array<clusterNode>) => !this.replication.manualMode() || this.replication.replicationFactor() > 0,
                message: `Please select at least one node.`
            }]
        });

        this.replication.replicationFactor.extend({
            required: true,
            validation: [
                {
                    validator: (val: number) => val >= 1 || this.replication.manualMode(),
                    message: `Replication factor must be at least 1.`
                },
                {
                    validator: (val: number) => val <= maxReplicationFactor,
                    message: `Max available nodes: {0}`,
                    params: maxReplicationFactor
                }
            ]
        });
    }
    
    private setupRestoreValidation() {
        this.restore.backupDirectory.extend({
            required: {
                onlyIf: () => this.creationMode === "restore" && this.restore.restorePoints().length === 0
            },
            validation: [
                {
                    validator: (_: string) => {
                        return this.creationMode === "restore" && !this.restore.backupDirectoryError();
                    },
                    message: "Couldn't fetch restore points, {0}",
                    params: this.restore.backupDirectoryError
                }
            ]
        });

        this.restore.selectedRestorePoint.extend({
            required: {
                onlyIf: () => this.creationMode === "restore"
            }
        });
    }
    
    private setupEncryptionValidation() {
        this.encryption.key.extend({
            required: true,
            base64: true //TODO: any other validaton ?
        });

        this.encryption.confirmation.extend({
            validation: [
                {
                    validator: (v: boolean) => v,
                    message: "Please confirm that you have saved the encryption key"
                }
            ]
        });
    }
    
    private setupLegacyMigrationValidation() {
        const migration = this.legacyMigration;

        migration.dataExporterFullPath.extend({
            required: true
        });

        migration.dataDirectory.extend({
            required: true
        });

        migration.sourceType.extend({
            required: true
        });

        migration.encryptionKey.extend({
            required: {
                onlyIf: () => migration.isEncrypted()
            }
        });

        migration.encryptionAlgorithm.extend({
            required: {
                onlyIf: () => migration.isEncrypted()
            }
        });

        migration.encryptionKeyBitsSize.extend({
            required: {
                onlyIf: () => migration.isEncrypted()
            }
        });
    }

    private topologyToDto(): Raven.Client.ServerWide.DatabaseTopology {
        const topology = {
            DynamicNodesDistribution: this.replication.dynamicMode()
        } as Raven.Client.ServerWide.DatabaseTopology;

        if (this.replication.manualMode()) {
            const nodes = this.replication.nodes();
            topology.Members = nodes.map(node => node.tag());
        }
        return topology;
    }

    useRestorePoint(restorePoint: Raven.Server.Documents.PeriodicBackup.RestorePoint) {
        this.restore.selectedRestorePoint(restorePoint.Key);
        this.restore.backupLocation(restorePoint.Details.Location);
        this.restore.lastFileNameToRestore(restorePoint.Details.FileName);
    }

    toDto(): Raven.Client.ServerWide.DatabaseRecord {
        const settings: dictionary<string> = {};
        const securedSettings: dictionary<string> = {};

        settings[configuration.core.dataDirectory] = _.trim(this.path.dataPath()) || null;

        return {
            DatabaseName: this.name(),
            Settings: settings,
            SecuredSettings: securedSettings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: this.topologyToDto()
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDocumentDto(): Raven.Client.ServerWide.PeriodicBackup.RestoreBackupConfiguration {
        const dataDirectory = _.trim(this.path.dataPath()) || null;

        return {
            DatabaseName: this.name(),
            BackupLocation: this.restore.backupLocation(),
            LastFileNameToRestore: this.restore.lastFileNameToRestore(),
            DataDirectory: dataDirectory,
            EncryptionKey: this.getEncryptionConfigSection().enabled() ? this.encryption.key() : null
        } as Raven.Client.ServerWide.PeriodicBackup.RestoreBackupConfiguration;
    }
    
    
    toOfflineMigrationDto(): Raven.Server.Smuggler.Migration.OfflineMigrationConfiguration {
        const migration = this.legacyMigration;
        return {
            DataDirectory: migration.dataDirectory(),
            DataExporterFullPath: migration.dataExporterFullPath(),
            BatchSize: migration.batchSize() || null, 
            IsRavenFs: migration.sourceType() === "ravenfs",
            IsCompressed: migration.isCompressed(),
            JournalsPath: migration.journalsPath(),
            DatabaseName: this.name(),
            EncryptionKey: migration.isEncrypted() ? migration.encryptionKey() : undefined,
            EncryptionAlgorithm: migration.isEncrypted() ? migration.encryptionAlgorithm() : undefined,
            EncryptionKeyBitsSize: migration.isEncrypted() ? migration.encryptionKeyBitsSize() : undefined
        } as Raven.Server.Smuggler.Migration.OfflineMigrationConfiguration;
    }
}

export = databaseCreationModel;
