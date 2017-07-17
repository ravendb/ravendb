import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");

class s3Settings extends amazonSettings {
    bucketName = ko.observable<string>();
    remoteFolderName = ko.observable<string>();

    constructor(dto: Raven.Client.Server.PeriodicBackup.S3Settings) {
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
        this.bucketName.extend({
            validation: [
                {
                    validator: (bucketName: string) => this.validate(() =>
                        !!bucketName && bucketName.length >= 4 && bucketName.length <= 62),
                    message: "Bucket name must be at least 3 characters long and no more than 63"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        !!bucketName && bucketName[0] !== "."),
                    message: "Bucket name cannot start with a period (.)"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        !!bucketName && bucketName[bucketName.length - 1] !== "."),
                    message: "Bucket name cannot end with a period (.)"
                },
                {
                    validator: (bucketName: string) => this.validate(() =>
                        !!bucketName && !!bucketName && !bucketName.includes("..")),
                    message: "There can be only one period between labels"
                },
                {
                    validator: (bucketName: string) => this.validate(() => !ipRegExp.test(bucketName)),
                    message: "Bucket name must not be formatted as an IP address (e.g., 192.168.5.4)"
                }
            ]
        });

        this.remoteFolderName.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.validationGroup = ko.validatedObservable({
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey,
            awsRegionName: this.awsRegionName,
            bucketName: this.bucketName,
            remoteFolderName: this.remoteFolderName
        });
    }

    validateS3BucketName(bucketName: string): boolean {
        const labels = bucketName.split(".");
        const labelRegExp = /^[a-z0-9-]+$/;
        const validLabel = (label: string) => {
            if (label == null || label.length === 0) {
                return false;
            }
            if (!labelRegExp.test(label)) {
                return false;
            }
            if (label.startsWith("-") || label.endsWith("-")) {
                return false;
            }

            return true;
        };

        if (labels.some(l => !validLabel(l))) {
            return true;
        }

        return false;
    }

    toDto(): Raven.Client.Server.PeriodicBackup.S3Settings {
        const dto = super.toDto() as Raven.Client.Server.PeriodicBackup.S3Settings;
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