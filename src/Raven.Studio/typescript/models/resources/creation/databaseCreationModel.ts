/// <reference path="../../../../typings/tsd.d.ts"/>
import configuration = require("configuration");
import restorePoint = require("./restorePoint");
import clusterNode = require("models/database/cluster/clusterNode");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");
import validateNameCommand = require("commands/resources/validateNameCommand");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import licenseModel = require("models/auth/licenseModel");
import getCloudBackupCredentialsFromLinkCommand = require("commands/resources/getCloudBackupCredentialsFromLinkCommand");
import backupCredentials = require("models/resources/creation/backupCredentials");
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import RestoreBackupConfigurationBase = Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;
import ShardedRestoreSettings = Raven.Client.Documents.Operations.Backups.Sharding.ShardedRestoreSettings;
import RestoreBackupConfiguration = Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration;
import shardingRestoreBackupDirectory from "models/resources/creation/shardingRestoreBackupDirectory";
import RestorePoints = Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints;
import DatabaseUtils from "components/utils/DatabaseUtils";
import moment from "moment";

type shardTopologyItem = {
    replicas: KnockoutObservableArray<clusterNode>;
}

interface RestorePointsGroup {
    databaseName: string;
    databaseNameTitle: string;
    restorePoints: restorePoint[];
}

class databaseCreationModel {
    static unknownDatabaseName = "Unknown Database";
    
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
        fetchingRestorePoints: ko.observable<boolean>(false),
        backupCredentialsLoading: ko.observable<boolean>(false)
    };
    
    lockActiveTab = ko.observable<boolean>(false);

    name = ko.observable<string>("");

    creationMode: dbCreationMode = null;
    isFromBackup: boolean;
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

        restorePoints: ko.observableArray<RestorePointsGroup>([]),
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
                    // eslint-disable-next-line @typescript-eslint/no-inferrable-types
                    const text: string = `Select restore point... (${count.toLocaleString()} ${count > 1 ? 'options' : 'option'})`;
                    return text;
                }
                
                // eslint-disable-next-line @typescript-eslint/no-inferrable-types
                const text: string = `${restorePoint.dateTime}, ${restorePoint.backupType()} Backup`;
                return text;
            }
        }),
        
        // sharding
        
        enableSharding: ko.observable<boolean>(false),
        shardingBackupDirectories: ko.observableArray<shardingRestoreBackupDirectory>([new shardingRestoreBackupDirectory()]),

        setShardingDirectoryNodeTag: (idx: number, nodeTag: string) => {
            this.restore.shardingBackupDirectories()[idx].nodeTag(nodeTag);
        },

        setShardingDirectoryPath: (idx: number, directoryPath: string) => {
            this.restore.shardingBackupDirectories()[idx].directoryPath(directoryPath);
            
            this.restoreSourceObject().getFolderPathOptions(directoryPath)
                .done((optionsList: string[]) => {
                    this.restore.shardingBackupDirectories()[idx].directoryPathOptions(optionsList);
                });
            
            this.fetchShardingRestorePoints();
        },
        
        addShardingDirectoryPath: () => {
            this.restore.shardingBackupDirectories.push(new shardingRestoreBackupDirectory());
            
            this.restoreSourceObject().getFolderPathOptions("")
                .done((optionsList: string[]) => {
                    this.restore.shardingBackupDirectories()[this.restore.shardingBackupDirectories().length - 1].directoryPathOptions(optionsList);
                });

            this.fetchShardingRestorePoints();
        },
        
        removeShardingDirectoryPath: (idx: number) => {
            this.restore.shardingBackupDirectories.splice(idx, 1);
            this.fetchShardingRestorePoints();
        }
    };

    restoreSourceObject: KnockoutComputed<backupCredentials.restoreSettings>; 
    
    restoreValidationGroup = ko.validatedObservable({ 
        selectedRestorePoint: this.restore.selectedRestorePoint,
        backupEncryptionKey: this.restore.backupEncryptionKey
    });
    
    replicationAndSharding = {
        replicationFactor: ko.observable<number>(2),
        manualMode: ko.observable<boolean>(false),
        dynamicMode: ko.observable<boolean>(true),
        nodes: ko.observableArray<clusterNode>([]),
        orchestrators: ko.observableArray<clusterNode>([]),
        enableSharding: ko.observable<boolean>(false),
        numberOfShards: ko.observable<number>(1),
        shardTopology: ko.observableArray<shardTopologyItem>([])
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

        this.restoreSourceObject = ko.pureComputed(() => {
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

        this.restoreSourceObject.subscribe(() => {
            this.clearRestorePoints();
            this.restore.restorePointError(null);
            this.fetchRestorePoints();
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

        this.restore.enableSharding.subscribe((toggleValue) => {
            if (toggleValue) {
                this.fetchShardingRestorePoints();
            } else {
                this.fetchRestorePoints();
            }
        });

        this.restoreSourceObject().getFolderPathOptions("")
            .done((optionsList: string[]) => {
                this.restore.shardingBackupDirectories()[0].directoryPathOptions(optionsList);
            })
        
        const methods: Array<keyof this & string>  = ["useRestorePoint", "dataPathHasChanged", "backupDirectoryHasChanged",
            "remoteFolderAzureChanged", "remoteFolderAmazonChanged", "remoteFolderGoogleCloudChanged"];
        
        _.bindAll(this, ...methods);
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
                this.clearRestorePoints();
            })
            .done((cloudCredentials) => {
                this.restore.ravenCloudCredentials().setCredentials(cloudCredentials);
                this.restore.ravenCloudCredentials().isBackupLinkValid(true);
                this.fetchRestorePoints();
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

    getRestorePointGroup(groups: RestorePointsGroup[], databaseName: string): RestorePointsGroup {
        return groups.find(x => x.databaseName === databaseName);
    }

    getRestorePoint(group: RestorePointsGroup, databaseName: string, date: string): restorePoint {
        const formattedDate = moment(date).format(generalUtils.dateFormat);

        return group.restorePoints.find(x => DatabaseUtils.shardGroupKey(x.databaseName()) === databaseName && x.dateTime === formattedDate);
    }

    async fetchShardingRestorePoints() {
        this.spinners.fetchingRestorePoints(true);
        this.restore.restorePointError(null);
        
        const groups: RestorePointsGroup[] = [];
        let shardNumber = 0;
        const directoryPaths = this.restore.shardingBackupDirectories().map(x => x.directoryPath());

        for await (const directoryPath of directoryPaths) {
            try {
                const restorePoints: RestorePoints = await this.restoreSourceObject()
                    .fetchRestorePointsCommand(directoryPath, shardNumber)
                    .execute();

                restorePoints.List.forEach(rp => {
                    const databaseName = DatabaseUtils.shardGroupKey(rp.DatabaseName);
                    
                    if (!this.getRestorePointGroup(groups, databaseName)) {
                        groups.push({ databaseName: databaseName, databaseNameTitle: "Database Name", restorePoints: [] });
                    }

                    const group = this.getRestorePointGroup(groups, databaseName);

                    if (!this.getRestorePoint(group, databaseName, rp.DateTime)) {
                        const newRestorePoint = new restorePoint(rp);
                        newRestorePoint.fileName = newRestorePoint.location = null;
                        
                        group.restorePoints.push(newRestorePoint);
                    }
                    
                    this.getRestorePoint(group, databaseName, rp.DateTime)
                        .availableShards.push({
                            shardNumber,
                            folderName: rp.Location,
                            lastFileNameToRestore: rp.FileName,
                        });
                });

            } catch (response) {
                const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                this.restore.restorePointError(generalUtils.trimMessage(messageAndOptionalException.message)); 
                this.restore.lastFailedFolderName = this.restoreSourceObject().folderContent();
                this.clearRestorePoints();
                this.restore.backupEncryptionKey("");

            } finally {
                this.spinners.fetchingRestorePoints(false);
                shardNumber++;
            }
        }

        this.restore.restorePoints(groups);
        this.restore.selectedRestorePoint(null);
        this.restore.backupEncryptionKey("");
        this.restore.restorePointError(null);
        this.restore.lastFailedFolderName = null;
        this.restore.restorePointsCount(groups.reduce((partialSum, group) => partialSum + group.restorePoints.length, 0));
    }
    
    fetchRestorePoints() {
        if (!this.restoreSourceObject().isValid()) {
            this.clearRestorePoints();
            return;
        }
        
        this.spinners.fetchingRestorePoints(true);
        this.restore.restorePointError(null);

        this.restoreSourceObject().fetchRestorePointsCommand() 
            .execute()
            .done((restorePoints: RestorePoints) => {
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
                    validator: () => this.restore.enableSharding() || this.restoreSourceObject().isValid(),
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
                            return (licenseModel.licenseStatus() && licenseModel.licenseStatus().HasEncryption);
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

    useRestorePoint(restorePoint: restorePoint) {
        this.restore.selectedRestorePoint(restorePoint);
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
                        Members: orchestrators
                    }
                }
            } : null
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDatabaseDto(): RestoreBackupConfiguration {
        const dataDirectory = _.trim(this.path.dataPath()) || null;
        const encryptDb = this.getEncryptionConfigSection().enabled();
        
        const restorePoint = this.restore.selectedRestorePoint();
        
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

        if (this.restore.enableSharding()) {
            const shardRestoreSettings: ShardedRestoreSettings = {
                Shards: Object.fromEntries(
                    this.restore.shardingBackupDirectories().map((x, idx) => [
                        idx,
                        {
                            ShardNumber: idx,
                            FolderName: restorePoint.availableShards[idx]?.folderName || null,
                            NodeTag: x.nodeTag(),
                            LastFileNameToRestore: restorePoint.availableShards[idx]?.lastFileNameToRestore || null
                        }
                    ])
                )
            };
            
            return {
                DatabaseName: this.name(),
                DisableOngoingTasks: this.restore.disableOngoingTasks(),
                SkipIndexes: this.restore.skipIndexes(),
                LastFileNameToRestore: null,
                DataDirectory: dataDirectory,
                EncryptionKey: databaseEncryptionKey,
                BackupEncryptionSettings: encryptionSettings,
                ShardRestoreSettings: shardRestoreSettings,
                BackupLocation: null
            };
        }

        const baseConfiguration: RestoreBackupConfigurationBase = {
            DatabaseName: this.name(),
            DisableOngoingTasks: this.restore.disableOngoingTasks(),
            SkipIndexes: this.restore.skipIndexes(),
            LastFileNameToRestore: restorePoint.fileName,
            DataDirectory: dataDirectory,
            EncryptionKey: databaseEncryptionKey,
            BackupEncryptionSettings: encryptionSettings,
            ShardRestoreSettings: null
        };
        
        return this.restoreSourceObject().getConfigurationForRestoreDatabase(baseConfiguration, restorePoint.location) as RestoreBackupConfiguration;
    }
}

export = databaseCreationModel;
