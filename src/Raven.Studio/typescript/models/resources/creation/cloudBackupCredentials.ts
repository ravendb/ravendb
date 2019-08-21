/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class cloudBackupCredentials {
    backupStorageType : string;
    backupStorageTypeText: string;   
   
    awsAccessKey: string;   
    awsSecretKey: string;
    awsSessionToken: string;
    
    awsRegionName: string;
    bucketName: string;
    remoteFolderName: string;

    timeLeft = ko.observable<moment.Moment>();
    timeLeftText: KnockoutComputed<string>;

    constructor(dto: federatedCredentials) {
        this.backupStorageType = dto.BackupStorageType;
        this.backupStorageTypeText = "Amazon " + this.backupStorageType;
        
        this.awsAccessKey = dto.AwsAccessKey;
        this.awsSecretKey = dto.AwsSecretKey;
        this.awsSessionToken = dto.AwsSessionToken;

        this.awsRegionName = dto.AwsRegionName;
        this.bucketName = dto.BucketName;
        this.remoteFolderName = dto.RemoteFolderName;

        this.timeLeft(moment.utc(moment(dto.Expires).format())); // kept as local time 
        this.timeLeftText = ko.pureComputed(() => {
            let timeLeftFormatted = generalUtils.formatDurationByDate(this.timeLeft(), false);
            return  timeLeftFormatted === 'less than a minute' ? 'Link has expired' : timeLeftFormatted;
        });
    }
    
    toDto() : Raven.Client.Documents.Operations.Backups.S3Settings {
        return {
            BucketName: this.bucketName,
            RemoteFolderName: this.remoteFolderName,
            AwsSessionToken: this.awsSessionToken,
            AwsSecretKey: this.awsSecretKey,
            AwsAccessKey: this.awsAccessKey,
            AwsRegionName: this.awsRegionName,
            Disabled: false,
            GetBackupConfigurationScript: null
        }
    }
}

export = cloudBackupCredentials;
