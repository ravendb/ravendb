import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

abstract class amazonSettings extends backupSettings {
    awsAccessKey = ko.observable<string>();
    awsSecretKey = ko.observable<string>();
    awsRegionName = ko.observable<string>();

    awsRegionLocation = ko.observable<string>();

    availableAwsRegionEndpoints = [
        { label: "US East (N. Virginia)", value: "us-east-1" },
        { label: "US East (Ohio)", value: "us-east-2" },
        { label: "US West (N. California)", value: "us-west-1" },
        { label: "US West (Oregon)", value: "us-west-2" },
        { label: "Canada", value: "ca-central-1" },
        { label: "EU (Ireland)", value: "eu-west-1" },
        { label: "EU (London)", value: "eu-west-2" },
        { label: "EU (Frankfurt)", value: "eu-central-1" },
        { label: "Asia Pacific (Tokyo)", value: "ap-northeast-1" },
        { label: "Asia Pacific (Seoul)", value: "ap-northeast-2" },
        { label: "Asia Pacific (Singapore)", value: "ap-southeast-1" },
        { label: "Asia Pacific (Sydney)", value: "ap-southeast-2" },
        { label: "Asia Pacific (Mumbai)", value: "ap-south-1" },
        { label: "South America (São Paulo)", value: "sa-east-1" }
    ];

    constructor(dto: Raven.Client.Server.PeriodicBackup.AmazonSettings) {
        super(dto);

        this.awsAccessKey(dto.AwsAccessKey);
        this.awsSecretKey(dto.AwsSecretKey);
        this.awsRegionName(dto.AwsRegionName);

        if (!!dto.AwsRegionName) {
            for (let i = 0; i < this.availableAwsRegionEndpoints.length; i++) {
                const endpoint = this.availableAwsRegionEndpoints[i];
                if (endpoint.value === dto.AwsRegionName.toLowerCase()) {
                    this.awsRegionLocation(endpoint.label);
                    break;
                }
            }
        }
        
        this.initAmazonValidation();
    }

    initAmazonValidation() {
        this.awsAccessKey.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.awsSecretKey.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.awsRegionName.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.credentialsValidationGroup = ko.validatedObservable({
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey,
            awsRegionName: this.awsRegionName
        });
    }

    useAwsRegion(awsRegionEndpoint: { label: string, value: string }) {
        this.awsRegionLocation(awsRegionEndpoint.label);
        this.awsRegionName(awsRegionEndpoint.value);
    }

    toDto(): Raven.Client.Server.PeriodicBackup.AmazonSettings {
        const dto: any = super.toDto();
        dto.AwsAccessKey = this.awsAccessKey();
        dto.AwsSecretKey = this.awsSecretKey();
        dto.AwsRegionName = this.awsRegionName();
        return dto;
    }
}

export = amazonSettings;