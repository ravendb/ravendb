/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");

abstract class restoreSettings {
    abstract backupStorageType: restoreSource;
    abstract backupStorageTypeText: string;
    abstract isValid(): boolean;
}

// *** Local ***
export class localServerCredentials implements restoreSettings {  
    backupStorageType: restoreSource = 'local'; 
    backupStorageTypeText: string ='Local Server Directory';
    
    backupDirectory = ko.observable<string>();

    static empty(): localServerCredentials {
        return new localServerCredentials();
    }
    
    isValid(): boolean {
        return !!_.trim(this.backupDirectory());
    }
}

// *** Amazon ***
export class amazonS3Credentials implements restoreSettings {
    backupStorageType: restoreSource = 'amazonS3';
    backupStorageTypeText: string ='Amazon S3';
    
    remoteFolder = ko.observable<string>();    
    accessKey = ko.observable<string>();
    secretKey = ko.observable<string>();
    regionName = ko.observable<string>();
    bucketName = ko.observable<string>();
    
    toDto() : Raven.Client.Documents.Operations.Backups.S3Settings
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
            GetBackupConfigurationScript: null
        }
    };

    static empty() : amazonS3Credentials{
        return new amazonS3Credentials();
    }

    isValid() : boolean {
        return !!_.trim(this.accessKey()) && !!_.trim(this.secretKey()) && !!_.trim(this.regionName()) && !!_.trim(this.bucketName());
    }

    useAwsRegion(awsRegionEndpoint: { label: string, value: string }) {
        this.regionName(amazonSettings.getDisplayRegionName(awsRegionEndpoint));
    }

    createAwsRegionAutoCompleter() {
        return ko.pureComputed(() => {
            let key = this.regionName();

            const options = amazonSettings.availableAwsRegionEndpointsStatic             
                .map(x => {
                    return {
                        label: x.label,
                        value: x.value
                    }
                });

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => amazonSettings.getDisplayRegionName(x).toLowerCase().includes(key));
            } else {
                return options;
            }
        });
    }
}

// *** Azure ***
export class azureCredentials implements restoreSettings {
    backupStorageType: restoreSource = 'azure';
    backupStorageTypeText: string ='Azure';
    
    remoteFolder = ko.observable<string>();    
    accountName = ko.observable<string>();
    accountKey = ko.observable<string>();
    container = ko.observable<string>();  
    
    toDto() : Raven.Client.Documents.Operations.Backups.AzureSettings
    {
        return {
            AccountKey: _.trim(this.accountKey()),
            AccountName: _.trim(this.accountName()),
            StorageContainer: _.trim(this.container()),
            RemoteFolderName: _.trim(this.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    };

    static empty() : azureCredentials{
        return new azureCredentials();
    }

    isValid() : boolean {
        return !!_.trim(this.accountName()) && !!_.trim(this.accountKey()) && !!_.trim(this.container());
    }
}

// *** Google Cloud ***
export class googleCloudCredentials implements restoreSettings {
    backupStorageType: restoreSource = 'googleCloud';
    backupStorageTypeText: string ='Google Cloud Platform';
    
    remoteFolder = ko.observable<string>();    
    bucketName = ko.observable<string>();
    googleCredentials = ko.observable<string>();

    toDto() : Raven.Client.Documents.Operations.Backups.GoogleCloudSettings
    {
        return {
            BucketName: _.trim(this.bucketName()),
            GoogleCredentialsJson: _.trim(this.googleCredentials()),
            RemoteFolderName: _.trim(this.remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    };

    static empty() : googleCloudCredentials{
        return new googleCloudCredentials();
    }

    isValid() : boolean {
        return !!_.trim(this.bucketName()) && !!_.trim(this.googleCredentials());
    }
}

// *** RavenDB Cloud ***
export class ravenCloudCredentials implements restoreSettings {
    backupStorageType: restoreSource = 'cloud';
    backupStorageTypeText: string ='RavenDB Cloud';
    
    backupLink = ko.observable<string>();
    isBackupLinkValid = ko.observable<boolean>();
    
    sessionToken: string;
    timeLeft = ko.observable<moment.Moment>(null);
    timeLeftText: KnockoutComputed<string>;
    
    amazonS3 = ko.observable<amazonS3Credentials>(amazonS3Credentials.empty());
    //azure = ko.observable<azureCredentials>(null);             // todo: add when Raven Cloud supports this
    //googleCloud = ko.observable<googleCloudCredentials>(null); // todo: add when Raven Cloud supports this

    constructor(dto: federatedCredentials) {
        
        this.setAmazonS3Credentials(dto);
        
        this.timeLeftText = ko.pureComputed(() => {
            if (this.timeLeft()) {
                let timeLeftFormatted = generalUtils.formatDurationByDate(this.timeLeft(), false);
                return timeLeftFormatted === 'less than a minute' ? 'Link has expired' : timeLeftFormatted;
            }
            return 'unknown';
        });
    }

    isValid() : boolean {
        return !!_.trim(this.backupLink()) && this.isBackupLinkValid();
    }
    
    setAmazonS3Credentials(credentialsDto: federatedCredentials) {
        this.amazonS3().accessKey(credentialsDto.AwsAccessKey);
        this.amazonS3().regionName(credentialsDto.AwsRegionName);
        this.amazonS3().secretKey(credentialsDto.AwsSecretKey);
        this.amazonS3().bucketName(credentialsDto.BucketName);
        this.amazonS3().remoteFolder(credentialsDto.RemoteFolderName);
        this.sessionToken = credentialsDto.AwsSessionToken;
        if (!!credentialsDto.Expires) {
            // moment.format will keep time as local time :)
            this.timeLeft(moment.utc(moment(credentialsDto.Expires).format()));
        }
    }
    
    toAmazonS3Dto() : Raven.Client.Documents.Operations.Backups.S3Settings {
        return {
            AwsSessionToken: this.sessionToken,
            AwsSecretKey: _.trim(this.amazonS3().secretKey()),
            AwsAccessKey: _.trim(this.amazonS3().accessKey()),
            AwsRegionName: _.trim(this.amazonS3().regionName()),
            BucketName: _.trim(this.amazonS3().bucketName()),
            RemoteFolderName: _.trim(this.amazonS3().remoteFolder()),
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    }     
    // toAzureDto()       // todo: add when Raven Cloud supports this
    // toGoogleCloudDto() // todo: add when Raven Cloud supports this

    static empty() : ravenCloudCredentials {
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
            GetBackupConfigurationScript: null
        });
    }
}
