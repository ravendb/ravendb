import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

abstract class amazonSettings extends backupSettings {
    awsAccessKey = ko.observable<string>();
    awsSecretKey = ko.observable<string>();
    awsRegionName = ko.observable<string>();

    selectedAwsRegion = ko.observable<string>();

    availableAwsRegionEndpoints = [
        { label: "Asia Pacific (Tokyo)", value: "ap-northeast-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Seoul)", value: "ap-northeast-2", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Mumbai)", value: "ap-south-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Singapore)", value: "ap-southeast-1", hasS3: true, hasGlacier: false },
        { label: "Asia Pacific (Sydney)", value: "ap-southeast-2", hasS3: true, hasGlacier: true },
        { label: "AWS GovCloud (US)", value: "us-gov-west-1", hasS3: true, hasGlacier: true },
        { label: "AWS GovCloud (US)", value: "fips-us-gov-west-1", hasS3: true, hasGlacier: false },
        { label: "Canada (Central)", value: "ca-central-1", hasS3: true, hasGlacier: true },
        { label: "China (Beijing)", value: "cn-north-1", hasS3: true, hasGlacier: true },
        { label: "EU (Frankfurt)", value: "eu-central-1", hasS3: true, hasGlacier: true },
        { label: "EU (Ireland)", value: "eu-west-1", hasS3: true, hasGlacier: true },
        { label: "EU (London)", value: "eu-west-2", hasS3: true, hasGlacier: true },
        { label: "South America (Sao Paulo)", value: "sa-east-1", hasS3: true, hasGlacier: false },
        { label: "US East (N. Virginia)", value: "us-east-1", hasS3: true, hasGlacier: true },
        { label: "US East (N. Virginia)", value: "external-1", hasS3: true, hasGlacier: false },
        { label: "US East (Ohio)", value: "us-east-2", hasS3: true, hasGlacier: true },
        { label: "US West (N. California)", value: "us-west-1", hasS3: true, hasGlacier: true },
        { label: "US West (Oregon)", value: "us-west-2", hasS3: true, hasGlacier: true }
    ];

    constructor(dto: Raven.Client.Server.PeriodicBackup.AmazonSettings) {
        super(dto);

        this.awsAccessKey(dto.AwsAccessKey);
        this.awsSecretKey(dto.AwsSecretKey);
        this.awsRegionName(dto.AwsRegionName);

        const lowerCaseRegionName = !dto.AwsRegionName ? "" : dto.AwsRegionName.toLowerCase();
        const region = this.availableAwsRegionEndpoints.find(x => x.value === lowerCaseRegionName);
        this.selectedAwsRegion(!!region ? this.getDisplayRegionName(region) : dto.AwsRegionName);

        this.initAmazonValidation();

        this.selectedAwsRegion.subscribe(newSelectedAwsRegion => {
            if (!newSelectedAwsRegion)
                return;

            const newSelectedAwsRegionLowerCase = newSelectedAwsRegion.toLowerCase();
            const foundRegion = this.availableAwsRegionEndpoints.find(x =>
                this.getDisplayRegionName(x).toLowerCase() === newSelectedAwsRegionLowerCase);
            if (foundRegion)
                return;

            this.awsRegionName(newSelectedAwsRegion.trim());
        });
    }

    initAmazonValidation() {
        this.awsRegionName.extend({
            required: {
                onlyIf: () => this.enabled()
            },
            validation: [
                {
                    validator: (awsRegionName: string) => this.validate(() => {
                        if (!awsRegionName)
                            return false;

                        const foundRegion = this.availableAwsRegionEndpoints.find(x =>
                            this.getDisplayRegionName(x).toLowerCase() === awsRegionName);
                        if (foundRegion)
                            return true;

                        if (!awsRegionName.includes("-") ||
                            awsRegionName.startsWith("-") ||
                            awsRegionName.endsWith("-"))
                            return false;

                        return true;
                    }),
                    message: "AWS Region must include a '-' and cannot start or end with it"
                }
            ]
        });

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

        this.credentialsValidationGroup = ko.validatedObservable({
            awsRegionName: this.awsRegionName,
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey
        });
    }

    createAwsRegionAutoCompleter(hasS3: boolean) {
        return ko.pureComputed(() => {
            let key = this.selectedAwsRegion();

            const options = this.availableAwsRegionEndpoints
                .filter(x => hasS3 ? x.hasS3 : x.hasGlacier)
                .map(x => {
                    return {
                        label: x.label,
                        value: x.value
                    }
                });

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => this.getDisplayRegionName(x).toLowerCase().includes(key));
            } else {
                return options;
            }
        });
    }

    useAwsRegion(awsRegionEndpoint: { label: string, value: string }) {
        this.selectedAwsRegion(this.getDisplayRegionName(awsRegionEndpoint));
        this.awsRegionName(awsRegionEndpoint.value);
    }

    private getDisplayRegionName(awsRegionEndpoint: { label: string, value: string }): string {
        return awsRegionEndpoint.label + " - " + awsRegionEndpoint.value;
    }

    toDto(): Raven.Client.Server.PeriodicBackup.AmazonSettings {
        const dto = super.toDto() as Raven.Client.Server.PeriodicBackup.AmazonSettings;
        dto.AwsAccessKey = this.awsAccessKey();
        dto.AwsSecretKey = this.awsSecretKey();
        dto.AwsRegionName = this.awsRegionName();
        return dto;
    }
}

export = amazonSettings;