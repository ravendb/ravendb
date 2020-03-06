import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");
import jsonUtil = require("common/jsonUtil");

class s3Settings extends amazonSettings {
    bucketName = ko.observable<string>();

    constructor(dto: Raven.Client.Documents.Operations.Backups.S3Settings, allowedRegions: Array<string>) {
        super(dto, "S3", allowedRegions);

        this.bucketName(dto.BucketName);

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.bucketName,
            this.awsAccessKey,
            this.awsSecretKey,
            this.awsRegionName,
            this.remoteFolderName,
            this.selectedAwsRegion,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initValidation() {
        /* Bucket name must :
            - be at least 3 and no more than 63 characters long.
            - be a series of one or more labels. 
                Adjacent labels are separated by a single period (.). 
                Bucket names can contain lowercase letters, numbers, and hyphens. 
                Each label must start and end with a lowercase letter or a number.
            - not be formatted as an IP address (e.g., 192.168.5.4).
        */

        const ipRegExp = /^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/;
        const letterOrNumberRegex = /^[a-z0-9]+$/;
        const regExp = /^[a-z0-9\.-]+$/;
        this.bucketName.extend({
            validation: [
                {
                    validator: (bucketName: string) => bucketName && bucketName.length >= 3 && bucketName.length <= 63,
                    message: "Bucket name should be between 3 and 63 characters long"
                },
                {
                    validator: (bucketName: string) => bucketName && regExp.test(bucketName),
                    message: "Allowed characters are lowercase characters, numbers, periods, and dashes"
                },
                {
                    validator: (bucketName: string) => bucketName && letterOrNumberRegex.test(bucketName[0]),
                    message: "Bucket name should start with a number or letter"
                },
                {
                    validator: (bucketName: string) => bucketName && letterOrNumberRegex.test(bucketName[bucketName.length - 1]),
                    message: "Bucket name should end with a number or letter"
                },
                {
                    validator: (bucketName: string) => bucketName && !bucketName.includes(".."),
                    message: "Bucket name cannot contain consecutive periods"
                },
                {
                    validator: (bucketName: string) => bucketName && !bucketName.includes(".-") && !bucketName.includes("-."),
                    message: "Bucket names cannot contain dashes next to periods (e.g. \" -.\" and/or \".-\")"
                },
                {
                    validator: (bucketName: string) => !ipRegExp.test(bucketName),
                    message: "Bucket name must not be formatted as an IP address (e.g., 192.168.5.4)"
                }
            ]
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey,
            awsRegionName: this.awsRegionName,
            bucketName: this.bucketName
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.S3Settings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.S3Settings;
        dto.BucketName = this.bucketName();
        return dto;
    }

    static empty(allowedRegions: Array<string>): s3Settings {
        return new s3Settings({
            Disabled: true,
            AwsAccessKey: null,
            AwsRegionName: null,
            AwsSecretKey: null,
            AwsSessionToken: null,
            BucketName: null,
            RemoteFolderName: null,
            GetBackupConfigurationScript: null,
            //TODO RavenDB-14716
            CustomServerUrl: null,
        }, allowedRegions);
    }
}

export = s3Settings;
