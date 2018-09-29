/// <reference path="../../../../typings/tsd.d.ts"/>
import configuration = require("configuration");
import restorePoint = require("models/resources/creation/restorePoint");
import clusterNode = require("models/database/cluster/clusterNode");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");
import validateNameCommand = require("commands/resources/validateNameCommand");

class databaseCreationModel {
    static unknownDatabaseName = "Unknown Database";

    readonly configurationSections: Array<availableConfigurationSection> = [
        {
            name: "Data source",
            id: "legacyMigration",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Backup source",
            id: "restore",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Encryption",
            id: "encryption",
            alwaysEnabled: false,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(false)
        },
        {
            name: "Replication",
            id: "replication",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        },
        {
            name: "Path",
            id: "path",
            alwaysEnabled: true,
            disableToggle: ko.observable<boolean>(false),
            enabled: ko.observable<boolean>(true)
        }
    ];

    spinners = {
        fetchingRestorePoints: ko.observable<boolean>(false)
    };
    
    lockActiveTab = ko.observable<boolean>(false);

    name = ko.observable<string>("");

    creationMode: dbCreationMode = null;
    isFromBackupOrFromOfflineMigration: boolean;

    restore = {
        backupDirectory: ko.observable<string>().extend({ throttle: 500 }),
        backupDirectoryError: ko.observable<string>(null),
        lastFailedBackupDirectory: null as string,
        selectedRestorePoint: ko.observable<restorePoint>(),
        selectedRestorePointText: ko.pureComputed<string>(() => {
            const restorePoint = this.restore.selectedRestorePoint();
            if (!restorePoint) {
                return null;
            }

            const text: string = `${restorePoint.dateTime}, ${restorePoint.backupType()} Backup`;
            return text;
        }),
        restorePoints: ko.observable<Array<{ databaseName: string, databaseNameTitle: string, restorePoints: restorePoint[] }>>([]),
        isFocusOnBackupDirectory: ko.observable<boolean>(),
        restorePointsCount: ko.observable<number>(0),
        disableOngoingTasks: ko.observable<boolean>(false),
        skipIndexesImport: ko.observable<boolean>(false),
        canSkipIndexesImport: ko.pureComputed<boolean>(() => {
            const restorePoint: restorePoint = this.restore.selectedRestorePoint();
            if (!restorePoint) {
                return true;
            }

            return !restorePoint.isSnapshotRestore;
        }),
        requiresEncryption: undefined as KnockoutComputed<boolean>
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

        this.restore.backupDirectory.subscribe((backupDirectory) => {
            this.fetchRestorePoints(backupDirectory, true);
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

            const backupDirectory = this.restore.backupDirectory();
            if (!this.restore.backupDirectory.isValid() &&
                backupDirectory === this.restore.lastFailedBackupDirectory)
                return;

            if (!backupDirectory)
                return;

            this.fetchRestorePoints(backupDirectory, false);
        });
        
        this.restore.selectedRestorePoint.subscribe(restorePoint => {
            const encryptionSection = this.getEncryptionConfigSection();
            this.lockActiveTab(true);
            try {
                if (restorePoint) {
                    if (restorePoint.isEncrypted) {
                        // turn on encryption
                        encryptionSection.disableToggle(true);
                        encryptionSection.enabled(true);
                    } else if (restorePoint.isSnapshotRestore && !restorePoint.isEncrypted) {
                        encryptionSection.disableToggle(true);
                        encryptionSection.enabled(false);
                    } else {
                        encryptionSection.disableToggle(false);
                        encryptionSection.enabled(false);
                    }
                } else {
                    encryptionSection.disableToggle(false);
                }
            } finally {
                this.lockActiveTab(false);    
            }
        });
        
        _.bindAll(this, "useRestorePoint");
    }

    private fetchRestorePoints(backupDirectory: string, skipReportingError: boolean) {
        if (!skipReportingError) {
            this.spinners.fetchingRestorePoints(true);
        }

        new getRestorePointsCommand(backupDirectory, skipReportingError)
            .execute()
            .done((restorePoints: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints) => {
                if (backupDirectory !== this.restore.backupDirectory()) {
                    // the backup directory changed
                    return;
                }

                const groups: Array<{ databaseName: string, databaseNameTitle: string, restorePoints: restorePoint[] }> = [];
                restorePoints.List.forEach(rp => {
                    const databaseName = rp.DatabaseName = rp.DatabaseName ? rp.DatabaseName : databaseCreationModel.unknownDatabaseName;
                    if (!groups.find(x => x.databaseName === databaseName)) {
                        const title = databaseName !== databaseCreationModel.unknownDatabaseName ? "Database Name" : "Unidentified folder format name";
                        groups.push({ databaseName: databaseName, databaseNameTitle: title, restorePoints: [] });
                    }

                    const group = groups.find(x => x.databaseName === databaseName);
                    group.restorePoints.push(new restorePoint(rp));
                });

                this.restore.restorePoints(groups);
                this.restore.selectedRestorePoint(null);
                this.restore.backupDirectoryError(null);
                this.restore.lastFailedBackupDirectory = null;
                this.restore.restorePointsCount(restorePoints.List.length);
            })
            .fail((response: JQueryXHR) => {
                if (backupDirectory !== this.restore.backupDirectory()) {
                    // the backup directory changed
                    return;
                }

                const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                this.restore.backupDirectoryError(generalUtils.trimMessage(messageAndOptionalException.message));
                this.restore.lastFailedBackupDirectory = this.restore.backupDirectory();
                this.restore.restorePoints([]);
                this.restore.restorePointsCount(0);
            })
            .always(() => this.spinners.fetchingRestorePoints(false));
    }

    getEncryptionConfigSection() {
        return this.configurationSections.find(x => x.id === "encryption");
    }

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>\|]*$/; // forbidden characters * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        const invalidPrefixCheck = (dbName: string) => {
            const dbToLower = dbName ? dbName.toLocaleLowerCase() : "";
            return !dbToLower.startsWith("~") && !dbToLower.startsWith("$home") && !dbToLower.startsWith("appdrive:");
        };

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
            }, 
            {
                validator: (val: string) => invalidPrefixCheck(val),
                message: "The path is illegal! Paths in RavenDB can't start with 'appdrive:', '~' or '$home'"
            }]
        });
    }

    setupValidation(databaseDoesntExist: (name: string) => boolean, maxReplicationFactor: number) {
        this.setupPathValidation(this.path.dataPath, "Data");

        const checkDatabaseName = (val: string,
                                   params: any,
                                   callback: (currentValue: string, result: string | boolean) => void) => {
            new validateNameCommand('Database', val)
                .execute()
                .done((result) => {
                    if (result.IsValid) {
                        callback(this.name(), true);
                    } else {
                        callback(this.name(), result.ErrorMessage);
                    }
                })
        };
        
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: (name: string) => databaseDoesntExist(name),
                    message: "Database already exists"
                },
                {
                    async: true,
                    validator: generalUtils.debounceAndFunnel(checkDatabaseName)
                }]
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
            ],
            digit: true
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

    useRestorePoint(restorePoint: restorePoint) {
        this.restore.selectedRestorePoint(restorePoint);
    }

    getRestorePointTitle(restorePoint: restorePoint) {
        return restorePoint.dateTime;
    }

    toDto(): Raven.Client.ServerWide.DatabaseRecord {
        const settings: dictionary<string> = {};
        const dataDir = _.trim(this.path.dataPath());

        if (dataDir) {
            settings[configuration.core.dataDirectory] = dataDir;
        }

        return {
            DatabaseName: this.name(),
            Settings: settings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: this.topologyToDto()
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDocumentDto(): Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration {
        const dataDirectory = _.trim(this.path.dataPath()) || null;

        return {
            DatabaseName: this.name(),
            BackupLocation: this.restore.selectedRestorePoint().location,
            DisableOngoingTasks: this.restore.disableOngoingTasks(),
            SkipIndexesImport: this.restore.canSkipIndexesImport() ? this.restore.skipIndexesImport() : false,
            LastFileNameToRestore: this.restore.selectedRestorePoint().fileName,
            DataDirectory: dataDirectory,
            EncryptionKey: this.getEncryptionConfigSection().enabled() ? this.encryption.key() : null
        } as Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration;
    }
    
    toOfflineMigrationDto(): Raven.Client.ServerWide.Operations.Migration.OfflineMigrationConfiguration {
        const migration = this.legacyMigration;
        return {
            DataDirectory: migration.dataDirectory(),
            DataExporterFullPath: migration.dataExporterFullPath(),
            BatchSize: migration.batchSize() || null,
            IsRavenFs: migration.sourceType() === "ravenfs",
            IsCompressed: migration.isCompressed(),
            JournalsPath: migration.journalsPath(),
            DatabaseRecord: this.toDto(),
            EncryptionKey: migration.isEncrypted() ? migration.encryptionKey() : undefined,
            EncryptionAlgorithm: migration.isEncrypted() ? migration.encryptionAlgorithm() : undefined,
            EncryptionKeyBitsSize: migration.isEncrypted() ? migration.encryptionKeyBitsSize() : undefined,
            OutputFilePath: null
        } as Raven.Client.ServerWide.Operations.Migration.OfflineMigrationConfiguration;
    }
}

export = databaseCreationModel;
