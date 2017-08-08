import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");

class s3Settings extends amazonSettings {
    bucketName = ko.observable<string>();
    remoteFolderName = ko.observable<string>();

    constructor(dto: Raven.Client.ServerWide.PeriodicBackup.S3Settings) {
        super(dto);

        this.bucketName(dto.BucketName);
        this.remoteFolderName(dto.RemoteFolderName);

        this.connectionType = "S3";
        this.initValidation();
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
                    validator: (bucketName: string) => this.validate(() =>
                        bucketName && bucketName.length >= 3 && bucketName.length <= 63),
                    message: "Bucket name should be between 3 and 63 characters long"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        bucketName && regExp.test(bucketName)),
                    message: "Allowed characters are lowercase characters, numbers, periods, and dashes"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        bucketName && letterOrNumberRegex.test(bucketName[0])),
                    message: "Bucket name should start with a number or letter"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        bucketName && letterOrNumberRegex.test(bucketName[bucketName.length - 1])),
                    message: "Bucket name should end with a number or letter"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        bucketName && !bucketName.includes("..")),
                    message: "Bucket name cannot contain consecutive periods"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        bucketName && !bucketName.includes(".-") && !bucketName.includes("-.")),
                    message: "Bucket names cannot contain dashes next to periods (e.g. \" -.\" and/or \".-\")"
                },
                {
                    validator: (bucketName: string) => this.validate(() => !ipRegExp.test(bucketName)),
                    message: "Bucket name must not be formatted as an IP address (e.g., 192.168.5.4)"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey,
            awsRegionName: this.awsRegionName,
            bucketName: this.bucketName
        });
    }

    toDto(): Raven.Client.ServerWide.PeriodicBackup.S3Settings {
        const dto = super.toDto() as Raven.Client.ServerWide.PeriodicBackup.S3Settings;
        dto.BucketName = this.bucketName();
        dto.RemoteFolderName = this.remoteFolderName();
        return dto;
    }

    static empty(): s3Settings {
        return new s3Settings({
            Disabled: true,
            AwsAccessKey: null,
            AwsRegionName: null,
            AwsSecretKey: null,
            BucketName: null,
            RemoteFolderName: null
        });
    }
}

export = s3Settings;