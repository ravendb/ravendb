class periodicBackupSetup {

    unsupported = ko.observable<boolean>(false);
    activated = ko.observable<boolean>(false);

    type = ko.observable<string>();
    mainValue = ko.observable<string>();

    awsAccessKey = ko.observable<string>();
    awsSecretKey = ko.observable<string>();
    awsRegionEndpoint = ko.observable<string>();

    azureStorageAccount = ko.observable<string>();
    azureStorageKey = ko.observable<string>();

    incrementalBackupInterval = ko.observable();
    incrementalBackupIntervalUnit = ko.observable();

    fullBackupInterval = ko.observable();
    fullBackupIntervalUnit = ko.observable();

    private dbSettingsDto: documentDto;

    private FILE_SYSTEM = "fileSystem";
    private GLACIER_VAULT = "glacierVault";
    private S3_BUCKET = "s3bucket";
    private AZURE_STORAGE = "azureStorage";
    private TU_MINUTES = "minutes";
    private TU_HOURS = "hours";
    private TU_DAYS = "days";

    availablePeriodicBackups = [
        { label: "File System Folder:", value: this.FILE_SYSTEM },
        { label: "Glacier Vault Name:", value: this.GLACIER_VAULT },
        { label: "S3 Bucket Name:", value: this.S3_BUCKET },
        { label: "Azure Storage Container:", value: this.AZURE_STORAGE }
    ];
    availableAwsRegionEndpoints = [
        { label: "US East (Virginia)", value: "us-east-1" },
        { label: "US West (N. California)", value: "us-west-1" },
        { label: "US West (Oregon)", value: "us-west-2" },
        { label: "EU West (Ireland)", value: "eu-west-1" },
        { label: "Asia Pacific (Tokyo)", value: "ap-northeast-1" },
        { label: "Asia Pacific (Singapore)", value: "ap-southeast-1" },
        { label: "South America (Sao Paulo)", value: "sa-east-1" }
    ];
    availableIntervalUnits = [this.TU_MINUTES, this.TU_HOURS, this.TU_DAYS];

    additionalAwsInfoRequired = ko.computed(function () {
        return jQuery.inArray(
            this.type(), [this.GLACIER_VAULT, this.S3_BUCKET]
            ) !== -1;
    }, this);


    additionalAzureInfoRequired = ko.computed(function () {
        return this.type() === this.AZURE_STORAGE;
    }, this);

    fromDto(dto: periodicBackupSetupDto) {

        this.awsRegionEndpoint(dto.AwsRegionEndpoint);

        this.setupTypeAndMainValue(dto);

        var incr = this.prepareBackupInterval(dto.IntervalMilliseconds);
        this.incrementalBackupInterval(incr[0]);
        this.incrementalBackupIntervalUnit(incr[1]);

        var full = this.prepareBackupInterval(dto.FullBackupIntervalMilliseconds);
        this.fullBackupInterval(full[0]);
        this.fullBackupIntervalUnit(full[1]);

        this.activated(true);
    }

    toDto(): periodicBackupSetupDto {
        return {
            GlacierVaultName: this.prepareMainValue(this.GLACIER_VAULT),
            S3BucketName: this.prepareMainValue(this.S3_BUCKET),
            AwsRegionEndpoint: this.awsRegionEndpoint(),
            AzureStorageContainer: this.prepareMainValue(this.AZURE_STORAGE),
            LocalFolderName: this.prepareMainValue(this.FILE_SYSTEM),
            IntervalMilliseconds: this.convertToMilliseconds(this.incrementalBackupInterval(), this.incrementalBackupIntervalUnit()),
            FullBackupIntervalMilliseconds: this.convertToMilliseconds(this.fullBackupInterval(), this.fullBackupIntervalUnit()),
        };
    }

    private prepareMainValue(expectedType: string): string {
        return ((this.type() === expectedType) ? this.mainValue() : null);
    }

    private convertToMilliseconds(value, unit): number {
        if (value && unit) {
            switch (unit) {
                case this.TU_MINUTES:
                    return value * 1000 * 60;
                case this.TU_HOURS:
                    return value * 1000 * 60 * 60;
                case this.TU_DAYS:
                    return value * 1000 * 60 * 60 * 24;
            }
        }
        return null;
    }

    fromDatabaseSettingsDto(dbSettingsDto: documentDto) {
        this.dbSettingsDto = dbSettingsDto;
        this.awsAccessKey(dbSettingsDto['Settings']['Raven/AWSAccessKey']);
        this.awsSecretKey(dbSettingsDto['SecuredSettings']['Raven/AWSSecretKey']);
        this.azureStorageAccount(dbSettingsDto['Settings']['Raven/AzureStorageAccount']);
        this.azureStorageKey(dbSettingsDto['SecuredSettings']['Raven/AzureStorageKey']);
    }

    toDatabaseSettingsDto(): documentDto {
        this.dbSettingsDto['Settings']['Raven/AWSAccessKey'] = this.awsAccessKey();
        this.dbSettingsDto['SecuredSettings']['Raven/AWSSecretKey'] = this.awsSecretKey();
        this.dbSettingsDto['Settings']['Raven/AzureStorageAccount'] = this.azureStorageAccount();
        this.dbSettingsDto['SecuredSettings']['Raven/AzureStorageKey'] = this.azureStorageKey();
        return this.dbSettingsDto;
    }

    private setupTypeAndMainValue(dto: periodicBackupSetupDto) {
        var count = 0;
        if (dto.LocalFolderName) {
            count += 1;
            this.type(this.FILE_SYSTEM);
            this.mainValue(dto.LocalFolderName);
        }
        if (dto.GlacierVaultName) {
            count += 1;
            this.type(this.GLACIER_VAULT);
            this.mainValue(dto.GlacierVaultName);
        }
        if (dto.S3BucketName) {
            count += 1;
            this.type(this.S3_BUCKET);
            this.mainValue(dto.S3BucketName);
        }
        if (dto.AzureStorageContainer) {
            count += 1;
            this.type(this.AZURE_STORAGE);
            this.mainValue(dto.AzureStorageContainer);
        }
        this.unsupported(count > 1);
    }

    private prepareBackupInterval(milliseconds) {
        if (milliseconds) {
            var seconds = milliseconds / 1000;
            var minutes = seconds / 60;
            var hours = minutes / 60;
            if (this.isValidTimeValue(hours)) {
                var days = hours / 24;
                if (this.isValidTimeValue(days)) {
                    return [days, this.TU_DAYS];
                }
                return [hours, this.TU_HOURS];
            }
            return [minutes, this.TU_MINUTES];
        }
        return [];
    }

    private isValidTimeValue(value: number): boolean {
        return value >= 1 && value % 1 === 0;
    }
}

export = periodicBackupSetup;