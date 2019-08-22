/// <reference path="../../../../typings/tsd.d.ts"/>
import configuration = require("configuration");
import restorePoint = require("models/resources/creation/restorePoint");
import clusterNode = require("models/database/cluster/clusterNode");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");
import validateNameCommand = require("commands/resources/validateNameCommand");
import validateOfflineMigration = require("commands/resources/validateOfflineMigration");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import licenseModel = require("models/auth/licenseModel");
import getCloudBackupCredentialsFromLinkCommand = require("commands/resources/getCloudBackupCredentialsFromLinkCommand");
import { localServerCredentials, 
         amazonS3Credentials, 
         azureCredentials, 
         googleCloudCredentials, 
         ravenCloudCredentials } from "models/resources/creation/backupCredentials";

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
        
        localServerCredentials: ko.observable<localServerCredentials>(localServerCredentials.empty()),
        azureCredentials: ko.observable<azureCredentials>(azureCredentials.empty()),
        amazonS3Credentials: ko.observable<amazonS3Credentials>(amazonS3Credentials.empty()),
        googleCloudCredentials: ko.observable<googleCloudCredentials>(googleCloudCredentials.empty()),
        ravenCloudCredentials: ko.observable<ravenCloudCredentials>(ravenCloudCredentials.empty()),

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
                switch (this.restore.source()) {
                    case 'local':
                        return "Enter Backup Directory above to continue";
                    case 'cloud':
                        return "Enter Backup Link above to continue";
                    case 'amazonS3':
                    case 'azure':
                    case 'googleCloud':
                        return 'Enter required fields above to continue';
                }
            } else {
                // case 3: Restore points found
                const restorePoint = this.restore.selectedRestorePoint();
                if (!restorePoint) {
                    const text : string  = `Select restore point... (${count.toLocaleString()} ${count > 1 ? 'options' : 'option'})`;
                    return text;
                }
                
                const text: string = `${restorePoint.dateTime}, ${restorePoint.backupType()} Backup`;
                return text;
            }
        })
    };
    
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

    replicationValidationGroup = ko.validatedObservable({
        replicationFactor: this.replication.replicationFactor,
        nodes: this.replication.nodes
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

        this.restore.source.subscribe(() =>  {
            this.clearRestorePoints(); 
            this.restore.restorePointError(null);
            
            let getRestorePoints = false;
            switch(this.restore.source()) {
                case 'local':
                    if (this.restore.localServerCredentials().isValid()) {
                        getRestorePoints = true;
                    }
                    break;
                case 'cloud':
                    if (this.restore.ravenCloudCredentials().isValid()) {
                        getRestorePoints = true;
                    }
                    break;
                case 'azure':
                    if (this.restore.azureCredentials().isValid()) {
                        getRestorePoints = true; }
                    break;
                case 'amazonS3':
                    if (this.restore.amazonS3Credentials().isValid())
                        getRestorePoints = true;
                    break;
                case 'googleCloud':
                    if (this.restore.googleCloudCredentials().isValid()) {
                        getRestorePoints = true;
                    }
                    break;
            }
            
            if (getRestorePoints) {
                this.fetchRestorePoints(true);
            }
        });
        
        // Local 
        this.restore.localServerCredentials().backupDirectory.throttle(300).subscribe(() => {            
            if (this.restore.localServerCredentials().isValid()) {
                this.fetchRestorePoints(true);
            }
            else {
                // This is mandatory only here because input for 'local' is only the 'folder' - 
                // as opposed to the other options where the restore points are  calculated based on the other 'inputs'
                this.clearRestorePoints();
                this.restore.restorePointError(null);
            }
        });
        
        // Azure
        this.restore.azureCredentials().accountName.throttle(300).subscribe(() => {
            this.fetchAzureRestorePoints();
        });
        this.restore.azureCredentials().accountKey.throttle(300).subscribe(() => {
            this.fetchAzureRestorePoints();
        });
        this.restore.azureCredentials().container.throttle(300).subscribe(() => {
            this.fetchAzureRestorePoints();
        });
        this.restore.azureCredentials().remoteFolder.throttle(300).subscribe(() => {
            if (this.restore.azureCredentials().isValid()) {
                this.fetchRestorePoints(true);
            }
        });
        
        // Google Cloud
        this.restore.googleCloudCredentials().bucketName.throttle(300).subscribe(() => {
            this.fetchGoogleCloudRestorePoints();
        });
        this.restore.googleCloudCredentials().googleCredentials.throttle(300).subscribe(() => {
            this.fetchGoogleCloudRestorePoints();
        });
        this.restore.googleCloudCredentials().remoteFolder.throttle(300).subscribe(() => {
            if (this.restore.googleCloudCredentials().isValid()) {
                this.fetchRestorePoints(true);
            }
        });

        // Amazon 
        this.restore.amazonS3Credentials().accessKey.throttle(300).subscribe(() => {
            this.fetchAmazonRestorePoints();
        });
        this.restore.amazonS3Credentials().secretKey.throttle(300).subscribe(() => {
            this.fetchAmazonRestorePoints();
        });
        this.restore.amazonS3Credentials().regionName.throttle(300).subscribe(() => {
            this.fetchAmazonRestorePoints();            
        });
        this.restore.amazonS3Credentials().bucketName.throttle(300).subscribe(() => {
            this.fetchAmazonRestorePoints();
        });
        this.restore.amazonS3Credentials().remoteFolder.throttle(300).subscribe(() => {
            if (this.restore.amazonS3Credentials().isValid()) {
                this.fetchRestorePoints(true);
            }
        });
        
        // Raven Cloud - Backup Link
        this.restore.ravenCloudCredentials().backupLink.throttle(300).subscribe(link => {
            if (!!_.trim(link)) {
                this.downloadCloudCredentials(link) 
            }
            else {
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

    private fetchAzureRestorePoints() {
        if (this.restore.source() === "azure" && this.restore.azureCredentials().isValid()) {
            this.fetchRestorePoints(true);
        } else {
            this.clearRestorePoints();
        }
    }

    private fetchGoogleCloudRestorePoints() {
        if (this.restore.source() === "googleCloud" && this.restore.googleCloudCredentials().isValid()) {
            this.fetchRestorePoints(true);
        } else {
            this.clearRestorePoints();
        }
    }

    private fetchAmazonRestorePoints() {
        if (this.restore.source() === "amazonS3" && this.restore.amazonS3Credentials().isValid()) {
            this.fetchRestorePoints(true);
        } else {
            this.clearRestorePoints();
        }
    }
    
    downloadCloudCredentials(link: string) {
        this.spinners.backupCredentialsLoading(true);
        
        new getCloudBackupCredentialsFromLinkCommand(link)
            .execute()
            .fail(() =>  {
                this.restore.ravenCloudCredentials().isBackupLinkValid(false);
                this.clearRestorePoints();
            })
            .done((cloudCredentials) => {
                // todo: decode the cloud credentials and set appropriate type accordingly - todo later when we support the other types
                this.restore.ravenCloudCredentials().setAmazonS3Credentials(cloudCredentials);
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

    private createRestorePointCommand(skipReportingError: boolean) {
        switch (this.restore.source()) {
            case "local":
                return getRestorePointsCommand.forServerLocal(this.restore.localServerCredentials().backupDirectory(), skipReportingError);
            case "cloud":
                return getRestorePointsCommand.forS3Backup(this.restore.ravenCloudCredentials().toAmazonS3Dto(), skipReportingError);
            case "azure":
                return getRestorePointsCommand.forAzureBackup(this.restore.azureCredentials().toDto(), skipReportingError);
            case "amazonS3":
                return getRestorePointsCommand.forS3Backup(this.restore.amazonS3Credentials().toDto(), skipReportingError);
            case "googleCloud":
                return getRestorePointsCommand.forGoogleCloudBackup(this.restore.googleCloudCredentials().toDto(), skipReportingError);
        }
    }
    
    private fetchRestorePoints(skipReportingError: boolean) {
        this.spinners.fetchingRestorePoints(true);
        this.restore.restorePointError(null);

        this.createRestorePointCommand(skipReportingError)
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
                
                switch (this.restore.source()) {
                    case 'local': this.restore.lastFailedFolderName = this.restore.localServerCredentials().backupDirectory(); break;
                    case 'cloud': this.restore.lastFailedFolderName = this.restore.ravenCloudCredentials().backupLink(); break;
                    case 'azure': this.restore.lastFailedFolderName = this.restore.azureCredentials().remoteFolder(); break;
                    case 'amazonS3': this.restore.lastFailedFolderName = this.restore.amazonS3Credentials().remoteFolder(); break;
                    case 'googleCloud': this.restore.lastFailedFolderName = this.restore.googleCloudCredentials().remoteFolder(); break;
                }

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
                    validator: () => {
                        switch (this.restore.source()) {
                            case 'local':
                                return this.restore.localServerCredentials().isValid();
                            case 'azure':
                                return this.restore.azureCredentials().isValid();
                            case 'amazonS3':
                                return this.restore.amazonS3Credentials().isValid();
                            case 'cloud':
                                return this.restore.ravenCloudCredentials().isValid();
                            case 'googleCloud':
                                return this.restore.googleCloudCredentials().isValid();
                        };
                        return true;
                    },
                    message: "Please enter valid source data"
                },
                {
                    validator: () => {
                        return !this.restore.restorePointError();
                    },
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

        return {
            DatabaseName: this.name(),
            Settings: settings,
            Disabled: false,
            Encrypted: this.getEncryptionConfigSection().enabled(),
            Topology: this.topologyToDto()
        } as Raven.Client.ServerWide.DatabaseRecord;
    }

    toRestoreDocumentDto(): Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase {
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

        switch (this.restore.source()) {
            case "local" :
                const localConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration;
                localConfiguration.BackupLocation = restorePoint.location;
                (localConfiguration as any as restoreTypeAware).Type = "Local" as Raven.Client.Documents.Operations.Backups.RestoreType; 
                return localConfiguration;
            case "cloud":
                const s3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
                s3Configuration.Settings = this.restore.ravenCloudCredentials().toAmazonS3Dto();
                (s3Configuration as any as restoreTypeAware).Type = "S3" as Raven.Client.Documents.Operations.Backups.RestoreType;
                return s3Configuration;
            case "azure":
                const azureConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromAzureConfiguration; 
                azureConfiguration.Settings = this.restore.azureCredentials().toDto();
                (azureConfiguration as any as restoreTypeAware).Type = "Azure" as Raven.Client.Documents.Operations.Backups.RestoreType;
                return azureConfiguration;
            case "amazonS3":
                const amazonS3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
                amazonS3Configuration.Settings = this.restore.amazonS3Credentials().toDto();
                (amazonS3Configuration as any as restoreTypeAware).Type = "S3" as Raven.Client.Documents.Operations.Backups.RestoreType;
                return amazonS3Configuration;
            case "googleCloud":
                const googleCloudConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromGoogleCloudConfiguration;
                googleCloudConfiguration.Settings = this.restore.googleCloudCredentials().toDto();
                (googleCloudConfiguration as any as restoreTypeAware).Type = "GoogleCloud" as Raven.Client.Documents.Operations.Backups.RestoreType;
                return googleCloudConfiguration;
            default:
                throw new Error("Unhandled source: " + this.restore.source());
        }
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
