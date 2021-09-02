import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");

abstract class amazonSettings extends backupSettings {
    awsAccessKey = ko.observable<string>();
    awsSecretKey = ko.observable<string>();
    awsRegionName = ko.observable<string>();
    remoteFolderName = ko.observable<string>();

    selectedAwsRegion = ko.observable<string>();
    
    allowedRegions: Array<string>;

    static availableAwsRegionEndpointsStatic = [
        { label: "Africa (Cape Town)", value: "af-south-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Hong Kong)", value: "ap-east-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Mumbai)", value: "ap-south-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Osaka-Local)", value: "ap-northeast-3", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Seoul)", value: "ap-northeast-2", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Singapore)", value: "ap-southeast-1", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Sydney)", value: "ap-southeast-2", hasS3: true, hasGlacier: true },
        { label: "Asia Pacific (Tokyo)", value: "ap-northeast-1", hasS3: true, hasGlacier: true },
        { label: "Canada (Central)", value: "ca-central-1", hasS3: true, hasGlacier: true },
        { label: "China (Beijing)", value: "cn-north-1", hasS3: true, hasGlacier: true },
        { label: "China (Ningxia)", value: "cn-northwest-1", hasS3: true, hasGlacier: true },
        { label: "Europe (Frankfurt)", value: "eu-central-1", hasS3: true, hasGlacier: true },
        { label: "Europe (Ireland)", value: "eu-west-1", hasS3: true, hasGlacier: true },
        { label: "Europe (London)", value: "eu-west-2", hasS3: true, hasGlacier: true },
        { label: "Europe (Milan)", value: "eu-south-1", hasS3: true, hasGlacier: true },
        { label: "Europe (Paris)", value: "eu-west-3", hasS3: true, hasGlacier: true },
        { label: "Europe (Stockholm)", value: "eu-north-1", hasS3: true, hasGlacier: true },
        { label: "Middle East (Bahrain)", value: "me-south-1", hasS3: true, hasGlacier: true },
        { label: "South America (São Paulo)", value: "sa-east-1", hasS3: true, hasGlacier: true },
        { label: "US East (N. Virginia)", value: "us-east-1", hasS3: true, hasGlacier: true },
        { label: "US East (Ohio)", value: "us-east-2", hasS3: true, hasGlacier: true },
        { label: "US West (N. California)", value: "us-west-1", hasS3: true, hasGlacier: true },
        { label: "US West (Oregon)", value: "us-west-2", hasS3: true, hasGlacier: true },
        { label: "AWS GovCloud (US-East)", value: "us-gov-east-1", hasS3: true, hasGlacier: true },
        { label: "AWS GovCloud (US-West)", value: "us-gov-west-1", hasS3: true, hasGlacier: true }
    ];

    availableAwsRegionEndpoints = amazonSettings.availableAwsRegionEndpointsStatic;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.AmazonSettings, 
                connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType,
                allowedRegions: Array<string>) {
        super(dto, connectionType);

        this.allowedRegions = allowedRegions;
        
        this.awsAccessKey(dto.AwsAccessKey);
        this.awsSecretKey(dto.AwsSecretKey);
        this.awsRegionName(dto.AwsRegionName);
        this.remoteFolderName(dto.RemoteFolderName || "");

        const lowerCaseRegionName = !dto.AwsRegionName ? "" : dto.AwsRegionName.toLowerCase();
        const region = this.availableAwsRegionEndpoints.find(x => x.value === lowerCaseRegionName);
        this.selectedAwsRegion(!!region ? amazonSettings.getDisplayRegionName(region) : dto.AwsRegionName);

        this.initAmazonValidation();

        this.selectedAwsRegion.subscribe(newSelectedAwsRegion => {
            if (!newSelectedAwsRegion) {
                this.awsRegionName(null);
                return;
            }

            const newSelectedAwsRegionLowerCase = newSelectedAwsRegion.toLowerCase();
            const foundRegion = this.availableAwsRegionEndpoints.find(x =>
                amazonSettings.getDisplayRegionName(x).toLowerCase() === newSelectedAwsRegionLowerCase);
            
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
            this.selectedAwsRegion,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initAmazonValidation() {
        
        this.awsRegionName.extend({
            required: {
                onlyIf: () => this.isRegionRequired()
            },
            validation: [
                {
                    validator: (awsRegionName: string) => !this.isRegionRequired() ||
                                                          (awsRegionName && (this.isRegionFound(awsRegionName) || !this.allowedRegions)),
                                                          // Allow custom regions only if administrator didn't restrict regions (if this.allowedRegions is null)
                    message: "Invalid region"
                },
                {
                    validator: (awsRegionName: string) => !this.isRegionRequired() || 
                                                          (awsRegionName && awsRegionName.includes("-") && !awsRegionName.startsWith("-") && !awsRegionName.endsWith("-")),
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
    }

    isRegionRequired() {
        return this.enabled();
    }

    private isRegionFound(region: string) {
        return this.availableAwsRegionEndpoints.find(x => x.value.toLowerCase() === region.toLowerCase());
    }
    
    createAwsRegionAutoCompleter(hasS3: boolean) {
        return ko.pureComputed(() => {
            let key = this.selectedAwsRegion();
            const options = this.availableAwsRegionEndpoints
                .filter(x => hasS3 ? x.hasS3 : x.hasGlacier)
                .filter(x => this.allowedRegions ? _.includes(this.allowedRegions, x.label) : true);

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => amazonSettings.getDisplayRegionName(x).toLowerCase().includes(key));
            } else {
                return options;
            }
        });
    }

    useAwsRegion(awsRegionEndpoint: { label: string, value: string }) {
        this.selectedAwsRegion(amazonSettings.getDisplayRegionName(awsRegionEndpoint));
    }

    static getDisplayRegionName(awsRegionEndpoint: { label: string, value: string }): string {
        return awsRegionEndpoint.label + " - " + awsRegionEndpoint.value;
    }

    toDto(): Raven.Client.Documents.Operations.Backups.AmazonSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.AmazonSettings;
        dto.AwsAccessKey = this.awsAccessKey();
        dto.AwsSecretKey = this.awsSecretKey();
        dto.AwsRegionName = this.awsRegionName();
        dto.RemoteFolderName = this.remoteFolderName() || null;
        return dto;
    }
}

export = amazonSettings;
