/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");
import s3Settings = require("viewmodels/database/tasks/destinations/s3Settings");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import commandBase = require("commands/commandBase");

export abstract class restoreSettings {
    backupStorageType: restoreSource;
    backupStorageTypeText: string;
    mandatoryFieldsText: string;

    folderContent: KnockoutComputed<string>;
    fetchRestorePointsCommand: () => commandBase;

    abstract getFolderPathOptions(): JQueryPromise<string[]>;
    
    abstract getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                                backupLocation: string): Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase
    abstract isValid(): boolean;
    
    abstract onCredentialsChange(onChange: () => void): void;

    protected getFolderPathOptionsByCommand(folderPathOptionsCommand: any): JQueryPromise<string[]> {
        const optionsListDeferred = $.Deferred<string[]>();
        
        folderPathOptionsCommand.execute()
            .done((result: Raven.Server.Web.Studio.FolderPathOptions) => optionsListDeferred.resolve(result.List));
        
        return optionsListDeferred;
    }
}

export class localServerCredentials extends restoreSettings {
    backupStorageType: restoreSource = 'local'; 
    backupStorageTypeText: string ='Local Server Directory';
    mandatoryFieldsText = "Backup Directory";

    backupDirectory = ko.observable<string>();
    folderContent = ko.pureComputed(() => this.backupDirectory());

    fetchRestorePointsCommand = () => getRestorePointsCommand.forServerLocal(this.backupDirectory(), true);

    getFolderPathOptions() {
        return super.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forServerLocal(this.backupDirectory(), true))
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const localConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreBackupConfiguration;
        localConfiguration.BackupLocation = backupLocation;
        (localConfiguration as any as restoreTypeAware).Type = "Local";
        return localConfiguration;
    }

    isValid(): boolean {
        return !!_.trim(this.backupDirectory());
    }

    onCredentialsChange(onChange: () => void) {
        this.backupDirectory.throttle(300).subscribe((onChange));
    }
    
    static empty(): localServerCredentials {
        return new localServerCredentials();
    }
}

export class amazonS3Credentials extends restoreSettings {
    backupStorageType: restoreSource = "amazonS3";
    backupStorageTypeText = "Amazon S3";
    mandatoryFieldsText = "required fields";
    
    remoteFolder = ko.observable<string>();
    folderContent = ko.pureComputed(() => this.remoteFolder());
    
    sessionToken = ko.observable<string>("");
    
    useCustomS3Host = ko.observable<boolean>(false);
    customServerUrl = ko.observable<string>();
    accessKey = ko.observable<string>();
    secretKey = ko.observable<string>();
    regionName = ko.observable<string>();
    bucketName = ko.observable<string>();
    accessKeyPropertyName = ko.pureComputed(() => s3Settings.getAccessKeyPropertyName(this.useCustomS3Host(), this.customServerUrl()));
    secretKeyPropertyName = ko.pureComputed(() => s3Settings.getSecretKeyPropertyName(this.useCustomS3Host(), this.customServerUrl()));

    toDto(): Raven.Client.Documents.Operations.Backups.S3Settings {
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
            RemoteFolderName: _.trim(this.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null,
            CustomServerUrl: this.useCustomS3Host() ? this.customServerUrl() : null,
        }
    };

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

    fetchRestorePointsCommand = () => getRestorePointsCommand.forS3Backup(this.toDto(), true);

    getFolderPathOptions() {
        return this.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(), "S3"));
    }

    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const amazonS3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
        amazonS3Configuration.Settings = this.toDto();
        amazonS3Configuration.Settings.RemoteFolderName = backupLocation;
        (amazonS3Configuration as any as restoreTypeAware).Type = "S3";
        return amazonS3Configuration;
    }

    isValid(): boolean {
        return !!_.trim(this.accessKey()) && !!_.trim(this.secretKey()) && !!_.trim(this.regionName()) && !!_.trim(this.bucketName()) && (!this.useCustomS3Host() || !!_.trim(this.customServerUrl()));
    }

    onCredentialsChange(onChange: () => void) {
        this.accessKey.throttle(300).subscribe(onChange);
        this.secretKey.throttle(300).subscribe(onChange);
        this.regionName.throttle(300).subscribe(onChange);
        this.bucketName.throttle(300).subscribe(onChange);
        this.remoteFolder.throttle(300).subscribe(onChange);
        this.customServerUrl.throttle(300).subscribe(onChange);
    }
    
    static empty(): amazonS3Credentials {
        return new amazonS3Credentials();
    }
    
    update(credentialsDto: federatedCredentials) {
        this.accessKey(credentialsDto.AwsAccessKey);
        this.regionName(credentialsDto.AwsRegionName);
        this.secretKey(credentialsDto.AwsSecretKey);
        this.bucketName(credentialsDto.BucketName);
        this.remoteFolder(credentialsDto.RemoteFolderName);
        this.sessionToken(credentialsDto.AwsSessionToken);
    }
}

export class azureCredentials extends restoreSettings {
    backupStorageType: restoreSource = "azure";
    backupStorageTypeText = "Azure";
    mandatoryFieldsText = "required fields";
    
    remoteFolder = ko.observable<string>();
    folderContent = ko.pureComputed(() => this.remoteFolder());
    
    accountName = ko.observable<string>();
    accountKey = ko.observable<string>();
    sasToken = ko.observable<string>();
    container = ko.observable<string>();
    
    toDto(): Raven.Client.Documents.Operations.Backups.AzureSettings {
        return {
            AccountKey: _.trim(this.accountKey()),
            SasToken: _.trim(this.sasToken()),
            AccountName: _.trim(this.accountName()),
            StorageContainer: _.trim(this.container()),
            RemoteFolderName: _.trim(this.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    };

    fetchRestorePointsCommand = () => getRestorePointsCommand.forAzureBackup(this.toDto(), true);

    getFolderPathOptions() {
        return super.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(), "Azure"))
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const azureConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromAzureConfiguration;
        azureConfiguration.Settings = this.toDto();
        azureConfiguration.Settings.RemoteFolderName = backupLocation;
        (azureConfiguration as any as restoreTypeAware).Type = "Azure";
        return azureConfiguration;
    }
    
    isValid(): boolean {
        return !!_.trim(this.accountName()) && !!_.trim(this.accountKey()) && !!_.trim(this.container());
    }
    
    onCredentialsChange(onChange: () => void) {
        this.accountKey.throttle(300).subscribe((onChange));
        this.sasToken.throttle(300).subscribe((onChange));
        this.accountName.throttle(300).subscribe(onChange);
        this.container.throttle(300).subscribe(onChange);
        this.remoteFolder.throttle(300).subscribe(onChange);
    }

    static empty(): azureCredentials {
        return new azureCredentials();
    }
    
    update(dto: AzureSasCredentials) {
        this.accountName(dto.AccountName);
        this.remoteFolder(dto.RemoteFolderName);
        this.sasToken(dto.SasToken);
        this.container(dto.StorageContainer);
    }
}

export class googleCloudCredentials extends restoreSettings {
    backupStorageType: restoreSource = 'googleCloud';
    backupStorageTypeText: string ='Google Cloud Platform';
    mandatoryFieldsText = "required fields";
    
    remoteFolder = ko.observable<string>();
    folderContent = ko.pureComputed(() => this.remoteFolder());
    
    bucketName = ko.observable<string>();
    googleCredentials = ko.observable<string>();

    toDto(): Raven.Client.Documents.Operations.Backups.GoogleCloudSettings {
        return {
            BucketName: _.trim(this.bucketName()),
            GoogleCredentialsJson: _.trim(this.googleCredentials()),
            RemoteFolderName: _.trim(this.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    };

    fetchRestorePointsCommand = () => getRestorePointsCommand.forGoogleCloudBackup(this.toDto(), true);

    getFolderPathOptions() {
        return this.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(), "GoogleCloud"))
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const googleCloudConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromGoogleCloudConfiguration;
        googleCloudConfiguration.Settings = this.toDto();
        googleCloudConfiguration.Settings.RemoteFolderName = backupLocation;
        (googleCloudConfiguration as any as restoreTypeAware).Type = "GoogleCloud";
        return googleCloudConfiguration;
    }
    
    isValid(): boolean {
        return !!_.trim(this.bucketName()) && !!_.trim(this.googleCredentials());
    }

    onCredentialsChange(onChange: () => void) {
        this.bucketName.throttle(300).subscribe((onChange));
        this.googleCredentials.throttle(300).subscribe(onChange);
        this.remoteFolder.throttle(300).subscribe(onChange);
    }
    
    static empty(): googleCloudCredentials{
        return new googleCloudCredentials();
    }
}

export class ravenCloudCredentials extends restoreSettings {
    backupStorageType: restoreSource = "cloud";
    backupStorageTypeText = "RavenDB Cloud";
    mandatoryFieldsText = "Backup Link";
    
    backupLink = ko.observable<string>();
    isBackupLinkValid = ko.observable<boolean>();
    folderContent = ko.pureComputed(() => this.backupLink());
    
    timeLeft = ko.observable<moment.Moment>(null);
    timeLeftText: KnockoutComputed<string>;

    cloudBackupProvider = ko.observable<BackupStorageType>();
    amazonS3 = ko.observable<amazonS3Credentials>(amazonS3Credentials.empty());
    azure = ko.observable<azureCredentials>(azureCredentials.empty());
    //googleCloud = ko.observable<googleCloudCredentials>(null); // todo: add when Raven Cloud supports this

    constructor() {
        super();
        
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
            RemoteFolderName: _.trim(s3.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null,
            //TODO RavenDB-14716
            CustomServerUrl: null,
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
            RemoteFolderName: azure.remoteFolder(),
            SasToken: azure.sasToken()
        } 
    }
    
    // toGoogleCloudDto() // todo: add when Raven Cloud supports this

    fetchRestorePointsCommand = () => {
        switch (this.cloudBackupProvider()) {
            case "S3":
                return getRestorePointsCommand.forS3Backup(this.toAmazonS3Dto(), true);
            case "Azure":
                return getRestorePointsCommand.forAzureBackup(this.toAzureDto(), true)
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
            case "S3":
                const s3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
                s3Configuration.Settings = this.toAmazonS3Dto();
                s3Configuration.Settings.RemoteFolderName = backupLocation;
                (s3Configuration as any as restoreTypeAware).Type = "S3";
                return s3Configuration;
            case "Azure":
                const azureConfiguration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromAzureConfiguration;
                azureConfiguration.Settings = this.toAzureDto();
                azureConfiguration.Settings.RemoteFolderName = backupLocation;
                (azureConfiguration as any as restoreTypeAware).Type = "Azure";
                return azureConfiguration;
            default:
                throw new Error("Unable to get restore configuration, unknown provider: " + this.cloudBackupProvider());
        }
    }
    
    isValid(): boolean {
        return !!_.trim(this.backupLink()) && this.isBackupLinkValid();
    }

    onCredentialsChange(onChange: (newValue: string) => void) {
        this.backupLink.throttle(300).subscribe((backupLinkNewValue) => onChange(backupLinkNewValue));
    }
    
    static empty() {
        return new ravenCloudCredentials();
    }
}
