import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");

abstract class amazonSettings extends backupSettings {
    awsAccessKey = ko.observable<string>();
    awsSecretKey = ko.observable<string>();
    awsRegionName = ko.observable<string>();

    selectedAwsRegion = ko.observable<string>();
    
    allowedRegions: Array<string>;

    availableAwsRegionEndpoints = [
        { label: "Asia Pacific (Mumbai)", value: "ap-south-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Seoul)", value: "ap-northeast-2", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Osaka-Local)", value: "ap-northeast-3", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Singapore)", value: "ap-southeast-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Sydney)", value: "ap-southeast-2", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Tokyo)", value: "ap-northeast-1", hasS3: true, hasGlacier: true },
        { label: "AWS GovCloud (US)", value: "us-gov-west-1", hasS3: true, hasGlacier: true },
        { label: "AWS GovCloud (US)", value: "fips-us-gov-west-1", hasS3: true, hasGlacier: false },
        { label: "Canada (Central)", value: "ca-central-1", hasS3: true, hasGlacier: true },
        { label: "China (Beijing)", value: "cn-north-1", hasS3: true, hasGlacier: true },
        { label: "China (Ningxia)", value: "cn-northwest-1", hasS3: true, hasGlacier: true },
        { label: "EU (Frankfurt)", value: "eu-central-1", hasS3: true, hasGlacier: true },
        { label: "EU (Ireland)", value: "eu-west-1", hasS3: true, hasGlacier: true },
        { label: "EU (London)", value: "eu-west-2", hasS3: true, hasGlacier: true },
        { label: "EU (Paris)", value: "eu-west-3", hasS3: true, hasGlacier: true },
        { label: "South America (Sao Paulo)", value: "sa-east-1", hasS3: true, hasGlacier: false },
        { label: "US East (N. Virginia)", value: "us-east-1", hasS3: true, hasGlacier: true },
        { label: "US East (N. Virginia)", value: "external-1", hasS3: true, hasGlacier: false },
        { label: "US East (Ohio)", value: "us-east-2", hasS3: true, hasGlacier: true },
        { label: "US West (N. California)", value: "us-west-1", hasS3: true, hasGlacier: true },
        { label: "US West (Oregon)", value: "us-west-2", hasS3: true, hasGlacier: true }
    ];

    constructor(dto: Raven.Client.Documents.Operations.Backups.AmazonSettings, 
                connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType,
                allowedRegions: Array<string>) {
        super(dto, connectionType);

        this.allowedRegions = allowedRegions;
        
        this.awsAccessKey(dto.AwsAccessKey);
        this.awsSecretKey(dto.AwsSecretKey);
        this.awsRegionName(dto.AwsRegionName);

        const lowerCaseRegionName = !dto.AwsRegionName ? "" : dto.AwsRegionName.toLowerCase();
        const region = this.availableAwsRegionEndpoints.find(x => x.value === lowerCaseRegionName);
        this.selectedAwsRegion(!!region ? this.getDisplayRegionName(region) : dto.AwsRegionName);

        this.initAmazonValidation();

        this.selectedAwsRegion.subscribe(newSelectedAwsRegion => {
            if (!newSelectedAwsRegion) {
                this.awsRegionName(null);
                return;
            }

            const newSelectedAwsRegionLowerCase = newSelectedAwsRegion.toLowerCase();
            const foundRegion = this.availableAwsRegionEndpoints.find(x =>
                this.getDisplayRegionName(x).toLowerCase() === newSelectedAwsRegionLowerCase);
            
            if (foundRegion) {
                // if we managed to find long name - set short name under the hood
                this.awsRegionName(foundRegion.value);
            } else {
                this.awsRegionName(newSelectedAwsRegion.trim());
            }
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.awsAccessKey,
            this.awsSecretKey,
            this.awsRegionName,
            this.selectedAwsRegion
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initAmazonValidation() {
        const self = this;
        this.awsRegionName.extend({
            required: {
                onlyIf: () => this.enabled()
            },
            validation: [
                {
                    validator: function (awsRegionName: string) {
                        return self.validate(() => {
                            if (!awsRegionName) {
                                return false;
                            }

                            const foundRegion = self.availableAwsRegionEndpoints.find(x =>
                                x.value.toLowerCase() === awsRegionName.toLowerCase());
                            if (foundRegion)
                                return true;

                            if (!awsRegionName.includes("-") ||
                                awsRegionName.startsWith("-") ||
                                awsRegionName.endsWith("-")) {
                                this.message = "AWS Region must include a '-' and cannot start or end with it";
                                return false;
                            }

                            // region wasn't found on the list
                            // we allow custom regions only if administrator didn't resticted regions.
                            if (self.allowedRegions) {
                                this.message = "Invalid region";
                                return false;
                            } else {
                                return true;
                            }
                        });
                    }
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
    }

    createAwsRegionAutoCompleter(hasS3: boolean) {
        return ko.pureComputed(() => {
            let key = this.selectedAwsRegion();

            const options = this.availableAwsRegionEndpoints
                .filter(x => hasS3 ? x.hasS3 : x.hasGlacier)
                .filter(x => this.allowedRegions ? _.includes(this.allowedRegions, x.value) : true)
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
    }

    private getDisplayRegionName(awsRegionEndpoint: { label: string, value: string }): string {
        return awsRegionEndpoint.label + " - " + awsRegionEndpoint.value;
    }

    toDto(): Raven.Client.Documents.Operations.Backups.AmazonSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.AmazonSettings;
        dto.AwsAccessKey = this.awsAccessKey();
        dto.AwsSecretKey = this.awsSecretKey();
        dto.AwsRegionName = this.awsRegionName();
        return dto;
    }
}

export = amazonSettings;
