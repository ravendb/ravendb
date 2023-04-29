/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");
import s3Settings = require("viewmodels/database/tasks/destinations/s3Settings");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import moment = require("moment");
import restorePoint from "models/resources/creation/restorePoint";
import recentError from "common/notifications/models/recentError";
import licenseModel from "models/auth/licenseModel";

const unknownDatabaseName = "Unknown Database";

class restoreItem {
    
    private readonly parent: restoreSettings;
    
    uniqueId = _.uniqueId("backup-dir");
    
    nodeTag = ko.observable<string>();
    
    folderName = ko.observable<string>(); // local folder or remote folder name

    backupFolderPathOptions = ko.observableArray<string>([]);
    
    restorePoints = ko.observableArray<RestorePointsGroup>([]);
    restorePointError = ko.observable<string>();
    selectedRestorePoint = ko.observable<restorePoint>();

    spinners = {
        fetchingRestorePoints: ko.observable(false)
    }
    
    validationGroup: KnockoutValidationGroup;

    restorePointButtonText = ko.pureComputed<string>(() => {
        const count = this.restorePoints().length;

        if (this.spinners.fetchingRestorePoints()) {
            // case 1: Loading restore points
            return "Loading restore points...";
        } else if (count === 0) {
            // case 2: No restore points fetched yet
            return `Enter required fields above to continue`;
        } else {
            // case 3: Restore points found
            const restorePoint = this.selectedRestorePoint();
            if (!restorePoint) {
                // eslint-disable-next-line @typescript-eslint/no-inferrable-types
                const text: string = `Select restore point... (${count.toLocaleString()} ${count > 1 ? 'options' : 'option'})`;
                return text;
            }

            // eslint-disable-next-line @typescript-eslint/no-inferrable-types
            const text: string = `${restorePoint.dateTime}, ${restorePoint.backupType()} Backup`;
            return text;
        }
    });
    
    constructor(parent: restoreSettings) {
        this.parent = parent;
        _.bindAll(this, "refreshPathAndRestorePoints", "updateBackupDirectoryPathOptions", "fetchRestorePoints");
        this.folderName.throttle(300).subscribe(this.refreshPathAndRestorePoints);
        
        this.initValidation();
    }
    
    shardNumber() {
        if (!this.parent.isShardedProvider()) {
            return undefined;
        }
        
        return this.parent.items().indexOf(this);
    }

    initValidation() {
        this.nodeTag.extend({
            required: {
                onlyIf: () => this.parent.isShardedProvider()
            }
        });

        this.folderName.extend({
            required: {
                onlyIf: () => this.restorePoints().length === 0
            }
        });

        this.selectedRestorePoint.extend({
            validation: [
                {
                    validator: () => !this.restorePointError(),
                    message: `Couldn't fetch restore points, {0}`,
                    params: this.restorePointError
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

        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
            selectedRestorePoint: this.selectedRestorePoint,
        });
    }
    
    refreshPathAndRestorePoints() {
        this.updateBackupDirectoryPathOptions();
        this.fetchRestorePoints();
    }
    
    updateBackupDirectoryPathOptions() {
        this.parent.getFolderPathOptions(this.folderName(), this.nodeTag())
            .done((optionsList: string[]) => this.backupFolderPathOptions(optionsList));
    }
    
    clearRestorePoints() {
        this.restorePoints([]);
        this.restorePointError(null);
    }
    
    isValid() {
        return !!_.trim(this.folderName());
    }

    fetchRestorePoints() {
        if (!this.isValid()) {
            return ;
        }
        
        this.restorePointError(null);
        this.spinners.fetchingRestorePoints(true);

        this.parent.fetchRestorePointsCommand(this, this.shardNumber()) 
            .execute()
            .done(restorePoints => {
                const groups: Array<{ databaseName: string, databaseNameTitle: string, restorePoints: restorePoint[] }> = [];
                restorePoints.List.forEach(rp => {
                    const databaseName = rp.DatabaseName = rp.DatabaseName ?? unknownDatabaseName;
                    if (!groups.find(x => x.databaseName === databaseName)) {
                        const title = databaseName !== unknownDatabaseName ? "Database Name" : "Unidentified folder format name";
                        groups.push({ databaseName: databaseName, databaseNameTitle: title, restorePoints: [] });
                    }

                    const group = groups.find(x => x.databaseName === databaseName);
                    group.restorePoints.push(new restorePoint(rp));
                });

                this.restorePoints(groups);
                this.selectedRestorePoint(null);
                this.restorePointError(null);
            })
            .fail((response: JQueryXHR) => {
                const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                this.restorePointError(generalUtils.trimMessage(messageAndOptionalException.message));
                this.clearRestorePoints();
            })
            .always(() => {
                this.spinners.fetchingRestorePoints(false);
            });
    }
}

export abstract class restoreSettings {
    backupStorageType: restoreSource;
    backupStorageTypeText: string;

    folderContent: KnockoutComputed<string>;
    
    items = ko.observableArray<restoreItem>([]);
    isShardedProvider: () => boolean;
    
    fetchRestorePointsCommand: (item: restoreItem, shardNumber: number) => getRestorePointsCommand;

    abstract isValid(): boolean;
    
    abstract getFolderPathOptions(folderName: string, nodeTag: string): JQueryPromise<string[]>;
    
    abstract getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                                backupLocation: string): Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase
    protected constructor(isShardedProvider: () => boolean) {
        this.isShardedProvider = isShardedProvider;
        _.bindAll(this, "addRestoreItem", "removeRestoreItem", "refreshPathAndRestorePoints");
        
        this.items.push(new restoreItem(this));
    }
    
    protected getFolderPathOptionsByCommand(folderPathOptionsCommand: any): JQueryPromise<string[]> {
        return folderPathOptionsCommand.execute()
            .then((result: Raven.Server.Web.Studio.FolderPathOptions) => result.List);
    }
    
    refreshPathAndRestorePoints() {
        this.updateBackupDirectoryPathOptions();
        this.fetchRestorePoints();
    }

    updateBackupDirectoryPathOptions() {
        this.items().forEach(item => {
            item.updateBackupDirectoryPathOptions();
        });
    }

    clearRestorePoints() {
        this.items().forEach(item => {
            item.clearRestorePoints();
        });
    }
    
    isItemsValid(): boolean {
        let valid = true;
        
        this.items().forEach(item => {
            if (!item.isValid()) {
                valid = false;
            }
            
            if (!item.validationGroup.isValid()) {
                item.validationGroup.errors.showAllMessages();
                valid = false;
            } 
        });
        
        return valid;
    }

    fetchRestorePoints() {
        if (!this.isValid()) {
            this.clearRestorePoints();
            return;
        }
        
        this.items().forEach((item) => {
            item.fetchRestorePoints();
        });
    }
    
    addRestoreItem() {
        const newItem = new restoreItem(this);
        this.items.push(newItem);
        
        newItem.refreshPathAndRestorePoints();
    }

    removeRestoreItem(item: restoreItem) {
        this.items.remove(item);
    }
}

export class localServerCredentials extends restoreSettings {
    backupStorageType: restoreSource = 'local'; 
    backupStorageTypeText ='Local Server Directory';

    fetchRestorePointsCommand = (item: restoreItem) => {
        const shardNumber = item.shardNumber();
        return getRestorePointsCommand.forServerLocal(item.folderName(), item.nodeTag(), true, shardNumber);
    };

    getFolderPathOptions(folderName: string, nodeTag: string) {
        return super.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forServerLocal(folderName, true, nodeTag));
    }

    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const localConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration;
        localConfiguration.BackupLocation = backupLocation;
        (localConfiguration as any as restoreTypeAware).Type = "Local";
        return localConfiguration;
    }

    isValid(): boolean {
        return true;
    }

    registerWatchers() {
        // empty by design 
    }
    
    static empty(isShardedProvider: () => boolean): localServerCredentials {
        return new localServerCredentials(isShardedProvider);
    }
}

export class amazonS3Credentials extends restoreSettings {
    backupStorageType: restoreSource = "amazonS3";
    backupStorageTypeText = "Amazon S3";
    
    sessionToken = ko.observable<string>("");
    
    useCustomS3Host = ko.observable<boolean>(false);
    customServerUrl = ko.observable<string>();
    forcePathStyle = ko.observable<boolean>(false);
    accessKey = ko.observable<string>();
    secretKey = ko.observable<string>();
    regionName = ko.observable<string>();
    bucketName = ko.observable<string>();
    accessKeyPropertyName = ko.pureComputed(() => s3Settings.getAccessKeyPropertyName(this.useCustomS3Host(), this.customServerUrl()));
    secretKeyPropertyName = ko.pureComputed(() => s3Settings.getSecretKeyPropertyName(this.useCustomS3Host(), this.customServerUrl()));

    toDto(remoteFolderName: string): Raven.Client.Documents.Operations.Backups.S3Settings {
        let selectedRegion = _.trim(this.regionName()).toLowerCase();
        const foundRegion = amazonSettings.availableAwsRegionEndpointsStatic.find(x => amazonSettings.getDisplayRegionName(x).toLowerCase() === selectedRegion);
        if (foundRegion) {
            selectedRegion = foundRegion.value;
        }
        
        return {
            AwsAccessKey: _.trim(this.accessKey()),
            AwsSecretKey: _.trim(this.secretKey()),
            AwsRegionName: selectedRegion,
            BucketName: _.trim(this.bucketName()),
            AwsSessionToken: this.sessionToken(),
            RemoteFolderName: remoteFolderName,
            Disabled: false,
            GetBackupConfigurationScript: null,
            CustomServerUrl: this.useCustomS3Host() ? this.customServerUrl() : null,
            ForcePathStyle: this.useCustomS3Host() ? this.forcePathStyle() : false,
        }
    }

    useAwsRegion(awsRegionEndpoint: { label: string, value: string }) {
        this.regionName(amazonSettings.getDisplayRegionName(awsRegionEndpoint));
    }

    createAwsRegionAutoCompleter() {
        return ko.pureComputed(() => {
            let key = this.regionName();
            const options = amazonSettings.availableAwsRegionEndpointsStatic;

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => amazonSettings.getDisplayRegionName(x).toLowerCase().includes(key));
            } else {
                return options;
            }
        });
    }

    fetchRestorePointsCommand = (item: restoreItem) => {
        const shardNumber = item.shardNumber();
        return getRestorePointsCommand.forS3Backup(this.toDto(item.folderName()), true, shardNumber);
    };

    getFolderPathOptions(folderName: string) {
        return this.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(folderName), "S3"));
    }

    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const amazonS3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
        amazonS3Configuration.Settings = this.toDto(backupLocation);
        (amazonS3Configuration as any as restoreTypeAware).Type = "S3";
        return amazonS3Configuration;
    }

    isValid(): boolean {
        const useCustomHost = this.useCustomS3Host();
        const isRegionValid = useCustomHost || !!_.trim(this.regionName()) 
        
        return !!_.trim(this.accessKey()) &&
               !!_.trim(this.secretKey()) &&
               isRegionValid &&
               !!_.trim(this.bucketName()) &&
               (!useCustomHost || !!_.trim(this.customServerUrl()));
    }

    registerWatchers() {
        const onChange = () => {
            this.updateBackupDirectoryPathOptions();
            this.fetchRestorePoints();
        }
        
        this.accessKey.throttle(300).subscribe(onChange);
        this.secretKey.throttle(300).subscribe(onChange);
        this.regionName.throttle(300).subscribe(onChange);
        this.bucketName.throttle(300).subscribe(onChange);
        this.customServerUrl.throttle(300).subscribe(onChange);
        this.forcePathStyle.throttle(300).subscribe(onChange);
    }
    
    static empty(isShardedProvider: () => boolean): amazonS3Credentials {
        return new amazonS3Credentials(isShardedProvider);
    }
    
    update(credentialsDto: federatedCredentials) {
        this.accessKey(credentialsDto.AwsAccessKey);
        this.regionName(credentialsDto.AwsRegionName);
        this.secretKey(credentialsDto.AwsSecretKey);
        this.bucketName(credentialsDto.BucketName);
        this.sessionToken(credentialsDto.AwsSessionToken);
        
        this.items()[0].folderName(credentialsDto.RemoteFolderName);
    }
}

export class azureCredentials extends restoreSettings {
    backupStorageType: restoreSource = "azure";
    backupStorageTypeText = "Azure";
    
    accountName = ko.observable<string>();
    accountKey = ko.observable<string>();
    sasToken = ko.observable<string>();
    container = ko.observable<string>();
    
    toDto(remoteFolderName: string): Raven.Client.Documents.Operations.Backups.AzureSettings {
        return {
            AccountKey: _.trim(this.accountKey()),
            SasToken: _.trim(this.sasToken()),
            AccountName: _.trim(this.accountName()),
            StorageContainer: _.trim(this.container()),
            RemoteFolderName: remoteFolderName,
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    }

    fetchRestorePointsCommand = (item: restoreItem) => {
        const shardNumber = item.shardNumber();
        return getRestorePointsCommand.forAzureBackup(this.toDto(item.folderName()), true, shardNumber);
    };

    getFolderPathOptions(folderName: string) {
        return super.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(folderName), "Azure"))
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const azureConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromAzureConfiguration;
        azureConfiguration.Settings = this.toDto(backupLocation);
        (azureConfiguration as any as restoreTypeAware).Type = "Azure";
        return azureConfiguration;
    }
    
    isValid(): boolean {
        return !!_.trim(this.accountName()) && !!_.trim(this.accountKey()) && !!_.trim(this.container());
    }
    
    registerWatchers() {
        const onChange = () => {
            this.updateBackupDirectoryPathOptions();
            this.fetchRestorePoints();
        }
        
        this.accountKey.throttle(300).subscribe(onChange);
        this.sasToken.throttle(300).subscribe(onChange);
        this.accountName.throttle(300).subscribe(onChange);
        this.container.throttle(300).subscribe(onChange);
    }

    static empty(isShardedProvider: () => boolean): azureCredentials {
        return new azureCredentials(isShardedProvider);
    }
    
    update(dto: AzureSasCredentials) {
        this.accountName(dto.AccountName);
        this.sasToken(dto.SasToken);
        this.container(dto.StorageContainer);
        
        this.items()[0].folderName(dto.RemoteFolderName);
    }
}

export class googleCloudCredentials extends restoreSettings {
    backupStorageType: restoreSource = 'googleCloud';
    backupStorageTypeText ='Google Cloud Platform';
    
    bucketName = ko.observable<string>();
    googleCredentials = ko.observable<string>();

    toDto(remoteFolderName: string): Raven.Client.Documents.Operations.Backups.GoogleCloudSettings {
        return {
            BucketName: _.trim(this.bucketName()),
            GoogleCredentialsJson: _.trim(this.googleCredentials()),
            RemoteFolderName: remoteFolderName,
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    }

    fetchRestorePointsCommand = (item: restoreItem) => {
        const shardNumber = item.shardNumber();
        return getRestorePointsCommand.forGoogleCloudBackup(this.toDto(item.folderName()), true, shardNumber);
    };

    getFolderPathOptions(folderName: string) {
        return this.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(folderName), "GoogleCloud"))
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const googleCloudConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromGoogleCloudConfiguration;
        googleCloudConfiguration.Settings = this.toDto(backupLocation);
        (googleCloudConfiguration as any as restoreTypeAware).Type = "GoogleCloud";
        return googleCloudConfiguration;
    }
    
    isValid(): boolean {
        return !!_.trim(this.bucketName()) && !!_.trim(this.googleCredentials());
    }

    registerWatchers() {
        const onChange = () => {
            this.updateBackupDirectoryPathOptions();
            this.fetchRestorePoints();
        }
        
        this.bucketName.throttle(300).subscribe(onChange);
        this.googleCredentials.throttle(300).subscribe(onChange);
    }
    
    static empty(isShardedProvider: () => boolean): googleCloudCredentials{
        return new googleCloudCredentials(isShardedProvider);
    }
}

export class ravenCloudCredentials extends restoreSettings {
    backupStorageType: restoreSource = "cloud";
    backupStorageTypeText = "RavenDB Cloud";
    
    backupLink = ko.observable<string>();
    isBackupLinkValid = ko.observable<boolean>();
    
    timeLeft = ko.observable<moment.Moment>(null);
    timeLeftText: KnockoutComputed<string>;

    cloudBackupProvider = ko.observable<BackupStorageType>();
    amazonS3 = ko.observable<amazonS3Credentials>(amazonS3Credentials.empty(() => this.isShardedProvider()));
    azure = ko.observable<azureCredentials>(azureCredentials.empty(() => this.isShardedProvider()));
    //googleCloud = ko.observable<googleCloudCredentials>(null); // todo: add when Raven Cloud supports this

    constructor(isShardedProvider: () => boolean) {
        super(isShardedProvider);
        
        //TODO: ???
        // this object and this.amazonS3 and this.azure contains list with restore points - let's connect this list to same space to unify approach
        this.amazonS3().items = this.items;
        this.azure().items = this.items;
        
        this.timeLeftText = ko.pureComputed(() => {
            const timeLeft = this.timeLeft();
            if (timeLeft) {
                if (timeLeft.isBefore()) {
                    return "Link has expired";
                }
                
                return generalUtils.formatDurationByDate(this.timeLeft());
            }
            return "unknown";
        });
    }
    
    setCredentials(credentialsDto: IBackupCredentials) {
        switch (credentialsDto.BackupStorageType) {
            case "S3":
                this.amazonS3().update(credentialsDto as federatedCredentials);
                break;
            case "Azure":
                this.azure().update(credentialsDto as AzureSasCredentials);
                break;
        }
        
        this.cloudBackupProvider(credentialsDto.BackupStorageType);
        
        if (credentialsDto.Expires) {
            this.timeLeft(moment.utc(credentialsDto.Expires));
        }
    }
    
    toAmazonS3Dto(): Raven.Client.Documents.Operations.Backups.S3Settings {
        const s3 = this.amazonS3();
        
        return {
            AwsSessionToken: _.trim(s3.sessionToken()),
            AwsSecretKey: _.trim(s3.secretKey()),
            AwsAccessKey: _.trim(s3.accessKey()),
            AwsRegionName: _.trim(s3.regionName()),
            BucketName: _.trim(s3.bucketName()),
            RemoteFolderName: this.items()[0].folderName(),
            Disabled: false,
            GetBackupConfigurationScript: null,
            //TODO RavenDB-14716
            CustomServerUrl: null,
            ForcePathStyle: false
        }
    }
    
    toAzureDto(): Raven.Client.Documents.Operations.Backups.AzureSettings {
        const azure = this.azure();
        
        return {
            StorageContainer: azure.container(),
            AccountKey: azure.accountKey(),
            AccountName: azure.accountName(),
            Disabled: false,
            GetBackupConfigurationScript: null,
            RemoteFolderName: this.items()[0].folderName(),
            SasToken: azure.sasToken()
        } 
    }
    
    // toGoogleCloudDto() // todo: add when Raven Cloud supports this

    fetchRestorePointsCommand = (item: restoreItem) => {
        const shardNumber = item.shardNumber();
        
        switch (this.cloudBackupProvider()) {
            case "S3":
                return getRestorePointsCommand.forS3Backup(this.toAmazonS3Dto(), true, shardNumber);
            case "Azure":
                return getRestorePointsCommand.forAzureBackup(this.toAzureDto(), true, shardNumber)
            default:
                throw new Error("Unable to fetch restore points, unknown provider: " + this.cloudBackupProvider());
        }
    };

    getFolderPathOptions() {
        // Folder options are not relevant when source is the 'RavenDB Cloud Link'.. 
        return $.Deferred<string[]>().reject();
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {

        switch (this.cloudBackupProvider()) {
            case "S3": {
                const s3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
                s3Configuration.Settings = this.toAmazonS3Dto();
                s3Configuration.Settings.RemoteFolderName = backupLocation;
                (s3Configuration as any as restoreTypeAware).Type = "S3";
                return s3Configuration;
            }
            case "Azure": {
                const azureConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromAzureConfiguration;
                azureConfiguration.Settings = this.toAzureDto();
                azureConfiguration.Settings.RemoteFolderName = backupLocation;
                (azureConfiguration as any as restoreTypeAware).Type = "Azure";
                return azureConfiguration;
            }
            default:
                throw new Error("Unable to get restore configuration, unknown provider: " + this.cloudBackupProvider());
        }
    }
    
    isValid(): boolean {
        return true; 
    }

    registerWatchers(onChange: (newValue: string) => void) {
        this.backupLink.throttle(300).subscribe((backupLinkNewValue) => onChange(backupLinkNewValue));
    }

    static empty(isShardedProvider: () => boolean) {
        return new ravenCloudCredentials(isShardedProvider);
    }
}
