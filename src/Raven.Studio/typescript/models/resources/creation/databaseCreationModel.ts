/// <reference path="../../../../typings/tsd.d.ts"/>
import configuration = require("configuration");
import clusterNode = require("models/database/cluster/clusterNode");
import generalUtils = require("common/generalUtils");
import validateNameCommand = require("commands/resources/validateNameCommand");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import getCloudBackupCredentialsFromLinkCommand = require("commands/resources/getCloudBackupCredentialsFromLinkCommand");
import backupCredentials = require("models/resources/creation/backupCredentials");
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import RestoreBackupConfigurationBase = Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;
import SingleShardRestoreSetting = Raven.Client.Documents.Operations.Backups.Sharding.SingleShardRestoreSetting;
import ShardedRestoreSettings = Raven.Client.Documents.Operations.Backups.Sharding.ShardedRestoreSettings;
import restorePoint from "models/resources/creation/restorePoint";

type shardTopologyItem = {
    replicas: KnockoutObservableArray<clusterNode>;
}

const defaultReplicationFactor = 2;

class databaseCreationModel {
    static storageExporterPathKeyName = storageKeyProvider.storageKeyFor("storage-exporter-path");

    readonly configurationSections: Array<availableConfigurationSection> = [
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
            name: "Replication & Sharding",
            id: "replicationAndSharding",
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
        backupCredentialsLoading: ko.observable<boolean>(false)
    };
    
    lockActiveTab = ko.observable<boolean>(false);

    name = ko.observable<string>("");

    creationMode: dbCreationMode = null;
    isFromBackup: boolean;
    canCreateEncryptedDatabases: KnockoutObservable<boolean>;

    restore = {
        enableSharding: ko.observable<boolean>(false),
        source: ko.observable<restoreSource>('local'),
        
        localServerCredentials: ko.observable<backupCredentials.localServerCredentials>(backupCredentials.localServerCredentials.empty(() => this.isSharded())),
        azureCredentials: ko.observable<backupCredentials.azureCredentials>(backupCredentials.azureCredentials.empty(() => this.isSharded())),
        amazonS3Credentials: ko.observable<backupCredentials.amazonS3Credentials>(backupCredentials.amazonS3Credentials.empty(() => this.isSharded())),
        googleCloudCredentials: ko.observable<backupCredentials.googleCloudCredentials>(backupCredentials.googleCloudCredentials.empty(() => this.isSharded())),
        ravenCloudCredentials: ko.observable<backupCredentials.ravenCloudCredentials>(backupCredentials.ravenCloudCredentials.empty(() => this.isSharded())),

        disableOngoingTasks: ko.observable<boolean>(false),
        skipIndexes: ko.observable<boolean>(false),
        requiresEncryption: undefined as KnockoutComputed<boolean>,
        backupEncryptionKey: ko.observable<string>(),
        
        restoreSourceObject: null as KnockoutComputed<backupCredentials.restoreSettings>
    };

    restoreValidationGroup = ko.validatedObservable({ 
        backupEncryptionKey: this.restore.backupEncryptionKey
    });
    
    replicationAndSharding = {
        replicationFactor: ko.observable<number>(defaultReplicationFactor),
        manualMode: ko.observable<boolean>(false),
        dynamicMode: ko.observable<boolean>(true),
        nodes: ko.observableArray<clusterNode>([]),
        orchestrators: ko.observableArray<clusterNode>([]),
        enableSharding: ko.observable<boolean>(false),
        numberOfShards: ko.observable<number>(1),
        shardTopology: ko.observableArray<shardTopologyItem>([{
            replicas: ko.observableArray(new Array(defaultReplicationFactor).fill(null))
        }])
    };
    
    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replicationAndSharding.replicationFactor,
        nodes: this.replicationAndSharding.nodes,
        orchestrators: this.replicationAndSharding.orchestrators,
        numberOfShards: this.replicationAndSharding.numberOfShards,
        shardTopology: this.replicationAndSharding.shardTopology
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
        this.isFromBackup = mode !== "newDatabase";

        this.restore.restoreSourceObject = ko.pureComputed(() => {
            switch (this.restore.source()) {
                case 'local': return this.restore.localServerCredentials();
                case 'cloud': return this.restore.ravenCloudCredentials();
                case 'amazonS3': return this.restore.amazonS3Credentials();
                case 'azure': return this.restore.azureCredentials();
                case 'googleCloud': return this.restore.googleCloudCredentials();
            }
        });
        
        const restoreConfig = this.configurationSections.find(x => x.id === "restore");
        restoreConfig.validationGroup = this.restoreValidationGroup;
        
        const encryptionConfig = this.getEncryptionConfigSection();
        encryptionConfig.validationGroup = this.encryptionValidationGroup;
        
        const replicationConfig = this.configurationSections.find(x => x.id === "replicationAndSharding");
        replicationConfig.validationGroup = this.replicationValidationGroup;

        const pathConfig = this.configurationSections.find(x => x.id === "path");
        pathConfig.validationGroup = this.pathValidationGroup;

        encryptionConfig.enabled.subscribe(() => {
            if (this.creationMode === "newDatabase") {
                this.replicationAndSharding.replicationFactor(this.replicationAndSharding.nodes().length);
            }
        });

        this.replicationAndSharding.nodes.subscribe(nodes => {
            this.replicationAndSharding.replicationFactor(nodes.length);
        });

        this.replicationAndSharding.replicationFactor.subscribe(factor => {
            if (factor === 1) {
                this.replicationAndSharding.dynamicMode(false);
            }
            
            this.assertShardTopologySpace();
        });
        
        this.replicationAndSharding.numberOfShards.subscribe(() => {
           this.assertShardTopologySpace();
        })

        // Raven Cloud - Backup Link 
        
        this.restore.ravenCloudCredentials().registerWatchers((backupLinkNewValue) => {
            if (_.trim(backupLinkNewValue)) {
                this.downloadCloudCredentials(backupLinkNewValue)
            } else {
                this.restore.googleCloudCredentials().clearRestorePoints();
            }
        });
        
        this.addOnRestorePointChanged(restorePoint => {
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
        })
        
        this.restore.restoreSourceObject.subscribe((o) => {
            o.refreshPathAndRestorePoints();
        });

        this.restore.restoreSourceObject().refreshPathAndRestorePoints();
        
        this.restore.enableSharding.subscribe((enable) => {
            this.restore.restoreSourceObject().refreshPathAndRestorePoints();
            
            // trunc restore locations when switching off the sharded mode. 
            if (!enable) {
                const items = this.restore.restoreSourceObject().items;
                if (items().length > 1) {
                    items([items()[0]]);
                }
            }
        });
        
        const methods: Array<keyof this & string>  = ["dataPathHasChanged", "isSharded"];
        
        _.bindAll(this, ...methods);
    }
    
    addOnRestorePointChanged(action: (restorePoint: restorePoint) => void) {
        this.restore.localServerCredentials().items()[0].selectedRestorePoint.subscribe(action);
        this.restore.azureCredentials().items()[0].selectedRestorePoint.subscribe(action);
        this.restore.amazonS3Credentials().items()[0].selectedRestorePoint.subscribe(action);
        this.restore.googleCloudCredentials().items()[0].selectedRestorePoint.subscribe(action);
        this.restore.ravenCloudCredentials().items()[0].selectedRestorePoint.subscribe(action);
    }
    
    isSharded(): boolean {
        return this.restore && this.restore.enableSharding();
    }
    
    private assertShardTopologySpace() {
        const factor = this.replicationAndSharding.replicationFactor();
        const shardCount = this.replicationAndSharding.numberOfShards();
        
        // assert we have space for manual sharding topology
        this.replicationAndSharding.shardTopology().forEach(shard => {
            const currentReplicas = shard.replicas.length;
            if (currentReplicas < factor) {
                Array.from(Array(factor - currentReplicas)).forEach(() => {
                    shard.replicas.push(null);
                });
            }
        });

        // assert we have space for manual sharding topology
        const currentLength = this.replicationAndSharding.shardTopology().length;
        if (currentLength < shardCount) {
            const replicationFactor = this.replicationAndSharding.replicationFactor();

            Array.from(Array(shardCount - currentLength)).forEach(() => {
                const newItem: shardTopologyItem = {
                    replicas: ko.observableArray(new Array(replicationFactor).fill(null))
                }
                this.replicationAndSharding.shardTopology.push(newItem);
            });
        }
    }
    
    downloadCloudCredentials(link: string) {
        this.spinners.backupCredentialsLoading(true);
        
        new getCloudBackupCredentialsFromLinkCommand(link)
            .execute()
            .fail(() => {
                this.restore.ravenCloudCredentials().isBackupLinkValid(false);
                this.restore.ravenCloudCredentials().clearRestorePoints();
            })
            .done((cloudCredentials) => {
                this.restore.ravenCloudCredentials().setCredentials(cloudCredentials);
                this.restore.ravenCloudCredentials().isBackupLinkValid(true);
                this.restore.ravenCloudCredentials().fetchRestorePoints();
            })
            .always(() => {
                this.spinners.backupCredentialsLoading(false);
            });
    }
    
    dataPathHasChanged(value: string) {
        this.path.dataPath(value);
        
        // try to continue autocomplete flow
        this.path.dataPathHasFocus(true);
    }
    
    getEncryptionConfigSection() {
        return this.configurationSections.find(x => x.id === "encryption");
    }

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>|]*$/; // forbidden characters * ? " < > |
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
        
        this.setupReplicationAndShardingValidation(maxReplicationFactor);
        this.setupEncryptionValidation();
        
        if (this.creationMode === "restore") {
            this.setupRestoreValidation();
        }
    }
    
    private setupReplicationAndShardingValidation(maxReplicationFactor: number) {
        this.replicationAndSharding.nodes.extend({
            validation: [{
                validator: () => !this.replicationAndSharding.manualMode() || this.replicationAndSharding.replicationFactor() > 0,
                message: `Please select at least one node.`
            }]
        });

        this.replicationAndSharding.orchestrators.extend({
            validation: [{
                validator: () => !this.replicationAndSharding.manualMode() || !this.replicationAndSharding.enableSharding() || this.replicationAndSharding.orchestrators().length > 0,
                message: `Please select at least one node.`
            }]
        });

        this.replicationAndSharding.replicationFactor.extend({
            required: true,
            validation: [
                {
                    validator: (val: number) => val >= 1 || this.replicationAndSharding.manualMode(),
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

        this.replicationAndSharding.numberOfShards.extend({
            required: () => this.replicationAndSharding.enableSharding(),
            validation: [
                {
                    validator: (val: number) => val >= 1,
                    message: `Number of shards must be at least 1.`
                }
            ],
            digit: true
        });
        
        this.replicationAndSharding.shardTopology.extend({
            required: true,
            validation: [
                {
                    validator: () => {
                        if (!this.replicationAndSharding.enableSharding() || !this.replicationAndSharding.manualMode()) {
                            return true;
                        }
                        
                        const shardTopology = this.replicationAndSharding.shardTopology();
                        const shards = this.replicationAndSharding.numberOfShards();
                        const replicas = this.replicationAndSharding.replicationFactor();
                        const shardInUse = shardTopology.slice(0, shards);
                        
                        return shardInUse.every(s => {
                            const replicasInUse = s.replicas().slice(0, replicas);
                            return replicasInUse.some(x => x);
                        });
                    },
                    message: "Each shard needs at least one replica"
                },
                {
                    validator: () => {
                        if (!this.replicationAndSharding.enableSharding() || !this.replicationAndSharding.manualMode()) {
                            return true;
                        }
                        
                        const shardTopology = this.replicationAndSharding.shardTopology();
                        const shards = this.replicationAndSharding.numberOfShards();
                        const replicas = this.replicationAndSharding.replicationFactor();
                        const shardInUse = shardTopology.slice(0, shards);

                        return shardInUse.every(s => {
                            const replicasInUse = s.replicas().slice(0, replicas);
                            const usedNodes = replicasInUse.filter(x => x).map(x => x.tag());
                            return usedNodes.length === new Set(usedNodes).size;
                        });
                    },
                    message: "Invalid shard topology - replicas must reside on different nodes"
                }
            ]
        })
    }
    
    private setupRestoreValidation() {
        this.restore.backupEncryptionKey.extend({
            required: {
                onlyIf: () => {
                    const restorePoint = this.restore.restoreSourceObject().items()[0].selectedRestorePoint();
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
    }
    
    private setupEncryptionValidation() {
        setupEncryptionKey.setupKeyValidation(this.encryption.key);
        setupEncryptionKey.setupConfirmationValidation(this.encryption.confirmation);
    }
    
    private getSavedDataExporterPath() {
        return localStorage.getItem(databaseCreationModel.storageExporterPathKeyName);
    }
    
    private topologyToDto(): Raven.Client.ServerWide.DatabaseTopology {
        const topology = {
            DynamicNodesDistribution: this.replicationAndSharding.dynamicMode()
        } as Raven.Client.ServerWide.DatabaseTopology;

        if (this.replicationAndSharding.manualMode()) {
            const nodes = this.replicationAndSharding.nodes();
            topology.Members = nodes.map(node => node.tag());
        }
        return topology;
    }
    
    toDto(): Raven.Client.ServerWide.DatabaseRecord {
        const settings: dictionary<string> = {};
        const dataDir = _.trim(this.path.dataPath());

        if (dataDir) {
            settings[configuration.core.dataDirectory] = dataDir;
        }

        const sharded = this.replicationAndSharding.enableSharding();
        const manualMode = this.replicationAndSharding.manualMode();
        const shards: Record<string, Raven.Client.ServerWide.DatabaseTopology> = {};
        const numberOfShards = this.replicationAndSharding.numberOfShards();
        
        if (numberOfShards && sharded) {
            for (let i = 0; i < numberOfShards; i++) {
                const shardTopology = {
                } as Raven.Client.ServerWide.DatabaseTopology;
                
                if (manualMode) {
                    const shardLayout = this.replicationAndSharding.shardTopology()[i].replicas().slice(0, numberOfShards);
                    shardTopology.Members = shardLayout.filter(x => x).map(x => x.tag());
                }
                
                shards[i.toString()] = shardTopology;
            }
        }
        
        const orchestrators = manualMode 
            ? this.replicationAndSharding.orchestrators().map(x => x.tag()) 
            : (clusterTopologyManager.default.topology()?.nodes().map(x => x.tag()) ?? []);
        
        return {
            DatabaseName: this.name(),
            Settings: settings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: sharded ? null : this.topologyToDto(),
            Sharding: sharded ? {
                Shards: shards,
                Orchestrator: {
                    Topology: {
                        Members: orchestrators[0] === "?" ? null : orchestrators
                    }
                }
            } : null
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDatabaseDto(): RestoreBackupConfigurationBase {
        const dataDirectory = _.trim(this.path.dataPath()) || null;
        const encryptDb = this.getEncryptionConfigSection().enabled();
        
        // here we make decision based on first item assuming all has same characteristics 
        const restorePoint = this.restore.restoreSourceObject().items()[0].selectedRestorePoint();
        
        let encryptionSettings: Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings = null;
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
        
        // we map no matter if restore is sharded or no - in case of non-sharded we simply using first item from result
        const perShardRestoreSettings = this.restore.restoreSourceObject().items().map((item, idx): SingleShardRestoreSetting => {
            return {
                FolderName: item.selectedRestorePoint().location,
                ShardNumber: idx,
                NodeTag: item.nodeTag(),
                LastFileNameToRestore: item.selectedRestorePoint().fileName
            }
        });
        
        const perShardAsMap: ShardedRestoreSettings = {
            Shards: perShardRestoreSettings.reduce<ShardedRestoreSettings["Shards"]>((p, c) => {
                p[c.ShardNumber] = c;
                return p;
            }, {})
        };
        
        const lastFileNameToRestore = this.isSharded() ? null : perShardRestoreSettings[0].LastFileNameToRestore;
        const backupLocation = this.isSharded() ? null : perShardRestoreSettings[0].FolderName;
        

        const baseConfiguration: RestoreBackupConfigurationBase = {
            DatabaseName: this.name(),
            DisableOngoingTasks: this.restore.disableOngoingTasks(),
            SkipIndexes: this.restore.skipIndexes(),
            LastFileNameToRestore: lastFileNameToRestore,
            DataDirectory: dataDirectory,
            EncryptionKey: databaseEncryptionKey,
            BackupEncryptionSettings: encryptionSettings,
            ShardRestoreSettings: this.isSharded() ? perShardAsMap : null
        };
        
        return this.restore.restoreSourceObject().getConfigurationForRestoreDatabase(baseConfiguration, backupLocation);
    }
}

export = databaseCreationModel;
