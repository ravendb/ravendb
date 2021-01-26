/// <reference path="../../../../typings/tsd.d.ts"/>
import configuration = require("configuration");
import restorePoint = require("models/resources/creation/restorePoint");
import clusterNode = require("models/database/cluster/clusterNode");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");
import validateNameCommand = require("commands/resources/validateNameCommand");
import validateOfflineMigration = require("commands/resources/validateOfflineMigration");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import licenseModel = require("models/auth/licenseModel");
import getCloudBackupCredentialsFromLinkCommand = require("commands/resources/getCloudBackupCredentialsFromLinkCommand");
import backupCredentials = require("models/resources/creation/backupCredentials");

class databaseCreationModel {
    static unknownDatabaseName = "Unknown Database";
    
    static storageExporterPathKeyName = storageKeyProvider.storageKeyFor("storage-exporter-path");

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
            name: "Sharding",
            id: "sharding",
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
        fetchingRestorePoints: ko.observable<boolean>(false),
        backupCredentialsLoading: ko.observable<boolean>(false)
    };
    
    lockActiveTab = ko.observable<boolean>(false);

    name = ko.observable<string>("");

    creationMode: dbCreationMode = null;
    isFromBackupOrFromOfflineMigration: boolean;
    canCreateEncryptedDatabases: KnockoutObservable<boolean>;

    restore = {
        source: ko.observable<restoreSource>('local'),
        
        localServerCredentials: ko.observable<backupCredentials.localServerCredentials>(backupCredentials.localServerCredentials.empty()),
        azureCredentials: ko.observable<backupCredentials.azureCredentials>(backupCredentials.azureCredentials.empty()),
        amazonS3Credentials: ko.observable<backupCredentials.amazonS3Credentials>(backupCredentials.amazonS3Credentials.empty()),
        googleCloudCredentials: ko.observable<backupCredentials.googleCloudCredentials>(backupCredentials.googleCloudCredentials.empty()),
        ravenCloudCredentials: ko.observable<backupCredentials.ravenCloudCredentials>(backupCredentials.ravenCloudCredentials.empty()),

        disableOngoingTasks: ko.observable<boolean>(false),
        skipIndexes: ko.observable<boolean>(false),
        requiresEncryption: undefined as KnockoutComputed<boolean>,
        backupEncryptionKey: ko.observable<string>(),
        lastFailedFolderName: null as string,

        restorePoints: ko.observable<Array<{ databaseName: string, databaseNameTitle: string, restorePoints: restorePoint[] }>>([]),
        restorePointsCount: ko.observable<number>(0),
        restorePointError: ko.observable<string>(),
        selectedRestorePoint: ko.observable<restorePoint>(),
        
        restorePointButtonText: ko.pureComputed<string>(() => {
            const count = this.restore.restorePointsCount();
            
            if (this.spinners.fetchingRestorePoints()) {
                // case 1: Loading restore points
                return "Loading restore points...";
            } else if (count === 0) {
                // case 2: No restore points fetched yet
                return `Enter ${this.restoreSourceObject().mandatoryFieldsText} above to continue`;
            } else {
                // case 3: Restore points found
                const restorePoint = this.restore.selectedRestorePoint();
                if (!restorePoint) {
                    const text: string = `Select restore point... (${count.toLocaleString()} ${count > 1 ? 'options' : 'option'})`;
                    return text;
                }
                
                const text: string = `${restorePoint.dateTime}, ${restorePoint.backupType()} Backup`;
                return text;
            }
        })
    };

    restoreSourceObject: KnockoutComputed<backupCredentials.restoreSettings>; 
    
    restoreValidationGroup = ko.validatedObservable({ 
        selectedRestorePoint: this.restore.selectedRestorePoint,
        backupEncryptionKey: this.restore.backupEncryptionKey
    });
    
    legacyMigration = {
        showAdvanced: ko.observable<boolean>(false),
        
        isEncrypted: ko.observable<boolean>(false),
        isCompressed: ko.observable<boolean>(false),

        dataDirectory: ko.observable<string>(),
        dataDirectoryHasFocus: ko.observable<boolean>(false),
        dataExporterFullPath: ko.observable<string>(),
        dataExporterFullPathHasFocus: ko.observable<boolean>(false),
        
        batchSize: ko.observable<number>(),
        sourceType: ko.observable<legacySourceType>(),
        journalsPath: ko.observable<string>(),
        journalsPathHasFocus: ko.observable<boolean>(false),
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
    
    sharding = {
        numberOfShards: ko.observable<number>(2), // ??? 1/2/3 ???
    }

    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replication.replicationFactor,
        nodes: this.replication.nodes
    });

    shardingValidationGroup = ko.validatedObservable({
        numbeOfShards: this.sharding.numberOfShards
    });

    path = {
        dataPath: ko.observable<string>(),
        dataPathHasFocus: ko.observable<boolean>(false)
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

    constructor(mode: dbCreationMode, canCreateEncryptedDatabases: KnockoutObservable<boolean>) {
        this.creationMode = mode;
        this.canCreateEncryptedDatabases = canCreateEncryptedDatabases;
        this.isFromBackupOrFromOfflineMigration = mode !== "newDatabase";

        this.restoreSourceObject = ko.pureComputed(() => {
            switch (this.restore.source()) {
                case 'local': return this.restore.localServerCredentials();
                case 'cloud': return this.restore.ravenCloudCredentials();
                case 'amazonS3': return this.restore.amazonS3Credentials();
                case 'azure': return this.restore.azureCredentials();
                case 'googleCloud': return this.restore.googleCloudCredentials();
            }
        });
        
        const legacyMigrationConfig = this.configurationSections.find(x => x.id === "legacyMigration");
        legacyMigrationConfig.validationGroup = this.legacyMigrationValidationGroup;
        
        const restoreConfig = this.configurationSections.find(x => x.id === "restore");
        restoreConfig.validationGroup = this.restoreValidationGroup;
        
        const encryptionConfig = this.getEncryptionConfigSection();
        encryptionConfig.validationGroup = this.encryptionValidationGroup;
        
        const replicationConfig = this.configurationSections.find(x => x.id === "replication");
        replicationConfig.validationGroup = this.replicationValidationGroup;

        const shardingConfig = this.configurationSections.find(x => x.id === "sharding");
        shardingConfig.validationGroup = this.shardingValidationGroup;

        const pathConfig = this.configurationSections.find(x => x.id === "path");
        pathConfig.validationGroup = this.pathValidationGroup;

        encryptionConfig.enabled.subscribe(() => {
            if (this.creationMode === "newDatabase") {
                this.replication.replicationFactor(this.replication.nodes().length);
            }
        });

        this.replication.nodes.subscribe(nodes => {
            this.replication.replicationFactor(nodes.length);
        });

        this.replication.replicationFactor.subscribe(factor => {
            if (factor === 1) {
                this.replication.dynamicMode(false);
            }
        });

        this.restoreSourceObject.subscribe(() => {
            this.clearRestorePoints();
            this.restore.restorePointError(null);
            this.fetchRestorePoints(true);
        });
        
        // Raven Cloud - Backup Link 
        this.restore.ravenCloudCredentials().onCredentialsChange((backupLinkNewValue) => {
            if (_.trim(backupLinkNewValue)) {
                this.downloadCloudCredentials(backupLinkNewValue)
            } else {
                this.clearRestorePoints();
            }
        });
        
        this.restore.selectedRestorePoint.subscribe(restorePoint => {
            const canCreateEncryptedDbs = this.canCreateEncryptedDatabases();
            
            const encryptionSection = this.getEncryptionConfigSection();
            this.lockActiveTab(true);
            try {
                if (restorePoint) {
                    if (restorePoint.isEncrypted) {
                        if (restorePoint.isSnapshotRestore) {
                            // encrypted snapshot - we are forced to encrypt newly created database 
                            // it requires license and https
                            
                            encryptionSection.enabled(true);
                            encryptionSection.disableToggle(true);
                        } else {
                            // encrypted backup - we need license and https for encrypted db
                            
                            encryptionSection.enabled(canCreateEncryptedDbs);
                            encryptionSection.disableToggle(!canCreateEncryptedDbs);
                        }
                    } else { //backup is not encrypted
                        if (restorePoint.isSnapshotRestore) {
                            // not encrypted snapshot - we can not create encrypted db
                            
                            encryptionSection.enabled(false);
                            encryptionSection.disableToggle(true);
                        } else {
                            // not encrypted backup - we need license and https for encrypted db
                            
                            encryptionSection.enabled(false);
                            encryptionSection.disableToggle(!canCreateEncryptedDbs); 
                        }
                    }
                } else {
                    encryptionSection.disableToggle(false);
                }
            } finally {
                this.lockActiveTab(false);
            }
        });
        
        _.bindAll(this, "useRestorePoint", "dataPathHasChanged", "backupDirectoryHasChanged", "remoteFolderAzureChanged", "remoteFolderAmazonChanged", "remoteFolderGoogleCloudChanged", 
            "legacyMigrationDataDirectoryHasChanged", "dataExporterPathHasChanged", "journalsPathHasChanged");
    }
    
    downloadCloudCredentials(link: string) {
        this.spinners.backupCredentialsLoading(true);
        
        new getCloudBackupCredentialsFromLinkCommand(link)
            .execute()
            .fail(() => {
                this.restore.ravenCloudCredentials().isBackupLinkValid(false);
                this.clearRestorePoints();
            })
            .done((cloudCredentials) => {
                this.restore.ravenCloudCredentials().setCredentials(cloudCredentials);
                this.restore.ravenCloudCredentials().isBackupLinkValid(true);
                this.fetchRestorePoints(true);
            })
            .always(() => this.spinners.backupCredentialsLoading(false));
    }

    backupDirectoryHasChanged(value: string) {
        this.restore.localServerCredentials().backupDirectory(value);
    }
    remoteFolderAzureChanged(value: string) {
        this.restore.azureCredentials().remoteFolder(value);
    }
    remoteFolderAmazonChanged(value: string) {
        this.restore.amazonS3Credentials().remoteFolder(value);
    }
    remoteFolderGoogleCloudChanged(value: string) {
        this.restore.googleCloudCredentials().remoteFolder(value);
    }
    
    dataPathHasChanged(value: string) {
        this.path.dataPath(value);
        
        // try to continue autocomplete flow
        this.path.dataPathHasFocus(true);
    }

    dataExporterPathHasChanged(value: string) {
        this.legacyMigration.dataExporterFullPath(value);
        
        //try to continue autocomplete flow
        this.legacyMigration.dataExporterFullPathHasFocus(true);
    }

    legacyMigrationDataDirectoryHasChanged(value: string) {
        this.legacyMigration.dataDirectory(value);
        
        //try to continue autocomplete flow
        this.legacyMigration.dataDirectoryHasFocus(true);
    }
    
    journalsPathHasChanged(value: string) {
        this.legacyMigration.journalsPath(value);
        
        //try to continue autocomplete flow
        this.legacyMigration.journalsPathHasFocus(true);
    }
    
    fetchRestorePoints(skipReportingError: boolean) {
        if (!this.restoreSourceObject().isValid()) {
            this.clearRestorePoints();
            return;
        }
        
        this.spinners.fetchingRestorePoints(true);
        this.restore.restorePointError(null);

        this.restoreSourceObject().fetchRestorePointsCommand() 
            .execute()
            .done((restorePoints: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints) => {
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
                this.restore.backupEncryptionKey("");
                this.restore.restorePointError(null);
                this.restore.lastFailedFolderName = null;
                this.restore.restorePointsCount(restorePoints.List.length);
            })
            .fail((response: JQueryXHR) => {
                const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                this.restore.restorePointError(generalUtils.trimMessage(messageAndOptionalException.message)); 
                this.restore.lastFailedFolderName = this.restoreSourceObject().folderContent();
                this.clearRestorePoints();
                this.restore.backupEncryptionKey("");
            })
            .always(() => this.spinners.fetchingRestorePoints(false));
    }

    private clearRestorePoints() {
        this.restore.restorePoints([]);
        this.restore.restorePointsCount(0);
        this.restore.selectedRestorePoint(null);
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
        this.setupShardingValidation();
        this.setupEncryptionValidation();
        
        if (this.creationMode === "restore") {
            this.setupRestoreValidation();
        }
        if (this.creationMode === "legacyMigration") {
            this.setupLegacyMigrationValidation();
        }
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
    
    private setupShardingValidation() {
        this.sharding.numberOfShards.extend({
            required: true,
            digit: true
        });
    }
    
    private setupRestoreValidation() {
        this.restore.backupEncryptionKey.extend({
            required: {
                onlyIf: () => {
                    const restorePoint = this.restore.selectedRestorePoint();
                    return restorePoint ? restorePoint.isEncrypted : false;
                }
            },
            base64: true
        });
        
        this.restore.source.extend({
            required: true
        });
        
        this.restore.ravenCloudCredentials().backupLink.extend({
            required: {
                onlyIf: (value: string) => _.trim(value) === ""
            },
            validation: [
                {
                    validator: () => this.spinners.backupCredentialsLoading() || this.restore.ravenCloudCredentials().isValid(),
                    message: "Failed to get link credentials"
                }
            ]
        });
        
        this.restore.localServerCredentials().backupDirectory.extend({
            required: {
                onlyIf: () => this.restore.restorePoints().length === 0 
            }
        });    
                
        this.restore.selectedRestorePoint.extend({
            validation: [
                {
                    validator: () => this.restoreSourceObject().isValid(),
                    message: "Please enter valid source data"
                },
                {
                    validator: () => !this.restore.restorePointError(),
                    message: `Couldn't fetch restore points, {0}`,
                    params: this.restore.restorePointError
                },
                {
                    validator: (restorePoint: restorePoint) => {
                        if (restorePoint && restorePoint.isEncrypted && restorePoint.isSnapshotRestore) {
                            // check if license supports that
                            return licenseModel.licenseStatus() && licenseModel.licenseStatus().HasEncryption;
                        }
                        return true;
                    },
                    message: "License doesn't support storage encryption"
                },
                {
                    validator: (value: string) => !!value,
                    message: "This field is required"
                }
            ]
        });
    }
    
    private setupEncryptionValidation() {
        setupEncryptionKey.setupKeyValidation(this.encryption.key);
        setupEncryptionKey.setupConfirmationValidation(this.encryption.confirmation);
    }
    
    private getSavedDataExporterPath() {
        return localStorage.getItem(databaseCreationModel.storageExporterPathKeyName);
    }
    
    private setupLegacyMigrationValidation() {
        const migration = this.legacyMigration;

        const checkDataExporterFullPath = (val: string, params: any, callback: (currentValue: string, result: string | boolean) => void) => {
            validateOfflineMigration.validateMigratorPath(migration.dataExporterFullPath())
                .execute()
                .done((response: Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation) => {
                    callback(migration.dataExporterFullPath(), response.IsValid || response.ErrorMessage);
                });
        };

        migration.dataExporterFullPath.extend({
            required: true,
            validation: {
                async: true,
                validator: generalUtils.debounceAndFunnel(checkDataExporterFullPath)
            }
        });
        
        const savedPath = this.getSavedDataExporterPath();
        if (savedPath) {
            migration.dataExporterFullPath(savedPath);
        }
        
        migration.dataExporterFullPath.subscribe(path => {
            localStorage.setItem(databaseCreationModel.storageExporterPathKeyName, path);
        });
        
        const checkDataDir = (val: string, params: any, callback: (currentValue: string, result: string | boolean) => void) => {
            validateOfflineMigration.validateDataDir(migration.dataDirectory())
                .execute()
                .done((response: Raven.Server.Web.Studio.StudioTasksHandler.OfflineMigrationValidation) => {
                    callback(migration.dataDirectory(), response.IsValid || response.ErrorMessage);
                });
        };

        migration.dataDirectory.extend({
            required: true,
            validation: {
                async: true,
                validator: generalUtils.debounceAndFunnel(checkDataDir)
            }
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

    toDto(): Raven.Client.ServerWide.DatabaseRecord {
        const settings: dictionary<string> = {};
        const dataDir = _.trim(this.path.dataPath());

        if (dataDir) {
            settings[configuration.core.dataDirectory] = dataDir;
        }

        const shards: Raven.Client.ServerWide.DatabaseTopology[] = [];
        const numberOfShards = this.sharding.numberOfShards();
        if (numberOfShards > 1) {
            for (let i = 0; i < numberOfShards; i++) {
                shards.push({} as Raven.Client.ServerWide.DatabaseTopology);
            }
        }
        
        return {
            DatabaseName: this.name(),
            Settings: settings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: numberOfShards > 1 ? null : this.topologyToDto(),
            Shards: shards
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDatabaseDto(): Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase {
        const dataDirectory = _.trim(this.path.dataPath()) || null;

        const restorePoint = this.restore.selectedRestorePoint();
        const encryptDb = this.getEncryptionConfigSection().enabled();
        
        let encryptionSettings = null as Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings;
        let databaseEncryptionKey = null;
        
        if (restorePoint.isEncrypted) {
            if (restorePoint.isSnapshotRestore) {
                if (encryptDb) {
                    encryptionSettings = {
                        EncryptionMode: "UseDatabaseKey",
                        Key: null
                    };
                    databaseEncryptionKey = this.restore.backupEncryptionKey();
                }
            } else { // backup of type backup
                encryptionSettings = {
                    EncryptionMode: "UseProvidedKey",
                    Key: this.restore.backupEncryptionKey()
                };
                
                if (encryptDb) {
                    databaseEncryptionKey = this.encryption.key();
                }
            }
        } else { // backup is not encrypted
            if (!restorePoint.isSnapshotRestore && encryptDb) {
                databaseEncryptionKey = this.encryption.key();
            }
        }
        
        const baseConfiguration = {
            DatabaseName: this.name(),
            DisableOngoingTasks: this.restore.disableOngoingTasks(),
            SkipIndexes: this.restore.skipIndexes(),
            LastFileNameToRestore: restorePoint.fileName,
            DataDirectory: dataDirectory,
            EncryptionKey: databaseEncryptionKey,
            BackupEncryptionSettings: encryptionSettings
        } as Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;
        
        return this.restoreSourceObject().getConfigurationForRestoreDatabase(baseConfiguration, restorePoint.location);
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
