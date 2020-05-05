/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");
import getRestorePointsCommand = require("commands/resources/getRestorePointsCommand");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import commandBase = require("commands/commandBase");

export abstract class restoreSettings {
    backupStorageType: restoreSource;
    backupStorageTypeText: string;
    mandatoryFieldsText: string;

    folderContent: KnockoutComputed<string>;
    fetchRestorePointsCommand: KnockoutComputed<commandBase>;    

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

// *** Local ***
export class localServerCredentials extends restoreSettings {  
    backupStorageType: restoreSource = 'local'; 
    backupStorageTypeText: string ='Local Server Directory';
    mandatoryFieldsText = "Backup Directory";

    backupDirectory = ko.observable<string>();
    folderContent = ko.pureComputed(() => this.backupDirectory());

    fetchRestorePointsCommand = ko.pureComputed(() => {
        return getRestorePointsCommand.forServerLocal(this.backupDirectory(), true);
    });

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

// *** Amazon ***
export class amazonS3Credentials extends restoreSettings {
    backupStorageType: restoreSource = 'amazonS3';
    backupStorageTypeText: string ='Amazon S3';
    mandatoryFieldsText = "required fields";
    
    remoteFolder = ko.observable<string>();
    folderContent = ko.pureComputed(() => this.remoteFolder());
    
    accessKey = ko.observable<string>();
    secretKey = ko.observable<string>();
    regionName = ko.observable<string>();
    bucketName = ko.observable<string>();
    
    toDto(): Raven.Client.Documents.Operations.Backups.S3Settings
    {
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
            AwsSessionToken: "",
            RemoteFolderName: _.trim(this.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null,
            //TODO RavenDB-14716
            CustomServerUrl: null,
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

    fetchRestorePointsCommand = ko.pureComputed(() => {
        return getRestorePointsCommand.forS3Backup(this.toDto(), true);
    });

    getFolderPathOptions() {
        return this.getFolderPathOptionsByCommand(getFolderPathOptionsCommand.forCloudBackup(this.toDto(), "S3"))
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
        return !!_.trim(this.accessKey()) && !!_.trim(this.secretKey()) && !!_.trim(this.regionName()) && !!_.trim(this.bucketName());
    }

    onCredentialsChange(onChange: () => void) {
        this.accessKey.throttle(300).subscribe(onChange);
        this.secretKey.throttle(300).subscribe(onChange);
        this.regionName.throttle(300).subscribe(onChange);
        this.bucketName.throttle(300).subscribe(onChange);
        this.remoteFolder.throttle(300).subscribe(onChange);
    }
    
    static empty(): amazonS3Credentials{
        return new amazonS3Credentials();
    }
}

// *** Azure ***
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
    
    toDto(): Raven.Client.Documents.Operations.Backups.AzureSettings
    {
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

    fetchRestorePointsCommand = ko.pureComputed(() => {
        return getRestorePointsCommand.forAzureBackup(this.toDto(), true);
    });

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
}

// *** Google Cloud ***
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

    fetchRestorePointsCommand = ko.pureComputed(() => {
        return getRestorePointsCommand.forGoogleCloudBackup(this.toDto(), true);
    });

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

// *** RavenDB Cloud ***
export class ravenCloudCredentials extends restoreSettings {
    backupStorageType: restoreSource = "cloud";
    backupStorageTypeText = "RavenDB Cloud";
    mandatoryFieldsText = "Backup Link";
    
    backupLink = ko.observable<string>();
    isBackupLinkValid = ko.observable<boolean>();
    folderContent = ko.pureComputed(() => this.backupLink());
    
    sessionToken: string;
    timeLeft = ko.observable<moment.Moment>(null);
    timeLeftText: KnockoutComputed<string>;
    
    amazonS3 = ko.observable<amazonS3Credentials>(amazonS3Credentials.empty());
    //azure = ko.observable<azureCredentials>(null);             // todo: add when Raven Cloud supports this
    //googleCloud = ko.observable<googleCloudCredentials>(null); // todo: add when Raven Cloud supports this

    constructor(dto: federatedCredentials) {
        super();
        
        this.setAmazonS3Credentials(dto);
        
        this.timeLeftText = ko.pureComputed(() => {
            if (this.timeLeft()) {
                const timeLeftFormatted = generalUtils.formatDurationByDate(this.timeLeft(), false);
                return timeLeftFormatted === 'less than a minute' ? 'Link has expired' : timeLeftFormatted;
            }
            return 'unknown';
        });
    }
    
    setAmazonS3Credentials(credentialsDto: federatedCredentials) {
        this.amazonS3().accessKey(credentialsDto.AwsAccessKey);
        this.amazonS3().regionName(credentialsDto.AwsRegionName);
        this.amazonS3().secretKey(credentialsDto.AwsSecretKey);
        this.amazonS3().bucketName(credentialsDto.BucketName);
        this.amazonS3().remoteFolder(credentialsDto.RemoteFolderName);
        this.sessionToken = credentialsDto.AwsSessionToken;
        if (credentialsDto.Expires) {
            // moment.format will keep time as local time :)
            this.timeLeft(moment.utc(moment(credentialsDto.Expires).format()));
        }
    }
    
    toAmazonS3Dto(): Raven.Client.Documents.Operations.Backups.S3Settings {
        return {
            AwsSessionToken: this.sessionToken,
            AwsSecretKey: _.trim(this.amazonS3().secretKey()),
            AwsAccessKey: _.trim(this.amazonS3().accessKey()),
            AwsRegionName: _.trim(this.amazonS3().regionName()),
            BucketName: _.trim(this.amazonS3().bucketName()),
            RemoteFolderName: _.trim(this.amazonS3().remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null,
            //TODO RavenDB-14716
            CustomServerUrl: null,
        }
    }     
    // toAzureDto()       // todo: add when Raven Cloud supports this
    // toGoogleCloudDto() // todo: add when Raven Cloud supports this

    fetchRestorePointsCommand = ko.pureComputed(() => {
        return getRestorePointsCommand.forS3Backup(this.toAmazonS3Dto(), true);
    });

    getFolderPathOptions() {
        // Folder options are not relevant when source is the 'RavenDB Cloud Link'.. 
        return $.Deferred<string[]>().reject();
    }
    
    getConfigurationForRestoreDatabase(baseConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase,
                                       backupLocation: string) {
        const s3Configuration = baseConfiguration as Raven.Client.Documents.Operations.Backups.RestoreFromS3Configuration;
        s3Configuration.Settings = this.toAmazonS3Dto();
        s3Configuration.Settings.RemoteFolderName = backupLocation;
        (s3Configuration as any as restoreTypeAware).Type = "S3";
        return s3Configuration;
    }
    
    isValid(): boolean {
        return !!_.trim(this.backupLink()) && this.isBackupLinkValid();
    }

    onCredentialsChange(onChange: (newValue: string) => void) {
        this.backupLink.throttle(300).subscribe((backupLinkNewValue) => onChange(backupLinkNewValue));
    }
    
    static empty(): ravenCloudCredentials {
        return new ravenCloudCredentials ({
            AwsAccessKey: null,
            AwsRegionName: null,
            AwsSecretKey: null,
            AwsSessionToken: null,
            RemoteFolderName: null,
            BucketName: null,
            Expires: null,
            BackupStorageType: null,
            Disabled: false,
            GetBackupConfigurationScript: null,
            //TODO RavenDB-14716
            CustomServerUrl: null,
        });
    }
}
