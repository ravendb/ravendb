class periodicExportSetup {

    static decryptFailedValue = "<data could not be decrypted>";

    onDiskExportEnabled = ko.observable<boolean>(false);
    remoteUploadEnabled = ko.observable<boolean>(false);

    localFolderName = ko.observable<string>();

    unsupported = ko.observable<boolean>(false);
    disabled = ko.observable<boolean>(true);

    type = ko.observable<string>();
    mainValue = ko.observable<string>();

    mainValueCustomValidity: KnockoutObservable<string>;

    azureRemoteFolderName = ko.observable<string>();
    s3RemoteFolderName = ko.observable<string>();

    awsAccessKey = ko.observable<string>(); 
    awsSecretKey = ko.observable<string>();
    awsRegionEndpoint = ko.observable<string>();

    awsSecretKeyDecryptionFailed = ko.observable<boolean>(false);
    azureStorageKeyDecryptionFailed = ko.observable<boolean>(false);

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

    availablePeriodicExports = [
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

	mainPlaceholder = ko.computed(() => {
		switch(this.type()) {
			case this.GLACIER_VAULT:
				return "vault name only e.g. myvault";
			case this.S3_BUCKET:
				return "bucket name only e.g. mybucket";
			case this.AZURE_STORAGE:
				return "container name only e.g. mycontainer";
		}
	}, this);

    additionalAwsInfoRequired = ko.computed(() => {
        var type = this.type();
        return this.remoteUploadEnabled() && jQuery.inArray(
            type, [this.GLACIER_VAULT, this.S3_BUCKET]
            ) !== -1;
    }, this);

    isGlaceirVault = ko.computed(() => this.remoteUploadEnabled() && this.type() === this.GLACIER_VAULT);
    isS3Bucket = ko.computed(() => this.remoteUploadEnabled() && this.type() === this.S3_BUCKET);

    additionalAzureInfoRequired = ko.computed(() => {
        var type = this.type();
        return this.remoteUploadEnabled() && type === this.AZURE_STORAGE;
    }, this);

    constructor() {
        this.mainValueCustomValidity = ko.computed(() => {
            var mainValue = this.mainValue();
            switch (this.type()) {
                case this.GLACIER_VAULT:
                    return this.validateGlacierVaultName(mainValue);
                case this.S3_BUCKET:
                    return this.validateS3Bucket(mainValue);
                case this.AZURE_STORAGE:
                    return this.validateAzureContainerNAme(mainValue);
            }

            return "";
        });
    }

    /*
    Names can be between 1 and 255 characters long.

    Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (hyphen), and '.' (period).
    */
    validateGlacierVaultName(vaultName: string): string {
        if (vaultName == null || vaultName.length < 1 || vaultName.length > 255) {
            return "Vault name must be at least 1 and no more than 255 characters long.";
        }
        var regEx = /^[A-Za-z0-9_\.-]+$/; 

        if (!regEx.test(vaultName)) {
            return "Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (hyphen), and '.' (period).";
        }

        return "";
    }

    /*
    Bucket names must :
        - be at least 3 and no more than 63 characters long.
        - be a series of one or more labels. 
            Adjacent labels are separated by a single period (.). 
            Bucket names can contain lowercase letters, numbers, and hyphens. 
            Each label must start and end with a lowercase letter or a number.
        - not be formatted as an IP address (e.g., 192.168.5.4).
    */
    validateS3Bucket(bucketName: string): string {

        if (bucketName == null || bucketName.length < 3 || bucketName.length > 63) {
            return "Bucket name must be at least 3 and no more than 63 characters long";
        }

        if (bucketName[0] == '.') {
            return 'Bucket name cannot start with a period (.)';
        }

        if (bucketName[bucketName.length - 1] == '.') {
            return 'Bucket name cannot end with a period (.)';
        }

        if (bucketName.indexOf("..") > -1) {
            return 'There can be only one period between labels';
        }

        var labels = bucketName.split(".");
        var labelRegExp = /^[a-z0-9-]+$/;
        var validLabel = label => {
            if (label == null || label.length == 0) {
                return false;
            }
            if (labelRegExp.test(label) == false) {
                return false;
            }
            if (label[0] == "-" || label[label.length - 1] == "-") {
                return false;
            }
            
            return true;
        };
        if (labels.some(l => !validLabel(l))) {
            return "Bucket name is invalid";
        }
        
        var ipRegExp = /^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/;
        if (ipRegExp.test(bucketName)) {
            return "Bucket name must not be formatted as an IP address (e.g., 192.168.5.4).";
        }

        return "";
    }

    /*
    Container names must:
        - start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
        - every dash (-) character must be immediately preceded and followed by a letter or number; 
        - consecutive dashes are not permitted in container names.
        - all letters in a container name must be lowercase.
        - container names must be from 3 through 63 characters long.
    */
    validateAzureContainerNAme(containerName: string): string {
        if (containerName == null || containerName.length < 3 || containerName.length > 63) {
            return "Container name must be at least 3 and no more than 63 characters long";
        }

        var regExp = /^[a-z0-9-]+$/;
        if (!regExp.test(containerName)) {
            return "Allowed characters are a-z,0-9 and '-' (hyphen).";
        }
        if (containerName[0] == "-" || containerName[containerName.length - 1] == "-") {
            return "Container name must start and end with a letter or number";
        }

        var twoDashes = /--/;
        if (twoDashes.test(containerName)) {
            return "Consecutive dashes are not permitted in container names";
        }

        return "";
    }

    fromDto(dto: periodicExportSetupDto) {
        this.awsRegionEndpoint(dto.AwsRegionEndpoint);

        this.setupTypeAndMainValue(dto);

        this.s3RemoteFolderName(dto.S3RemoteFolderName);
        this.azureRemoteFolderName(dto.AzureRemoteFolderName);
        var incr = this.prepareBackupInterval(dto.IntervalMilliseconds);
        this.incrementalBackupInterval(incr[0]);
        this.incrementalBackupIntervalUnit(incr[1]);

        var full = this.prepareBackupInterval(dto.FullBackupIntervalMilliseconds);
        this.fullBackupInterval(full[0]);
        this.fullBackupIntervalUnit(full[1]);

        this.disabled(dto.Disabled);
    }

    toDto(): periodicExportSetupDto {
        return {
            Disabled: this.disabled(),
            GlacierVaultName: this.prepareMainValue(this.GLACIER_VAULT),
            S3BucketName: this.prepareMainValue(this.S3_BUCKET),
            AwsRegionEndpoint: this.awsRegionEndpoint(),
            AzureStorageContainer: this.prepareMainValue(this.AZURE_STORAGE),
            LocalFolderName: this.onDiskExportEnabled() ? this.localFolderName() : null,
            S3RemoteFolderName: this.isS3Bucket() ? this.s3RemoteFolderName() : null,
            AzureRemoteFolderName: this.additionalAzureInfoRequired() ? this.azureRemoteFolderName() : null,
            IntervalMilliseconds: this.convertToMilliseconds(this.incrementalBackupInterval(), this.incrementalBackupIntervalUnit()),
            FullBackupIntervalMilliseconds: this.convertToMilliseconds(this.fullBackupInterval(), this.fullBackupIntervalUnit()),
        };
    }

    private prepareMainValue(expectedType: string): string {
        return ((this.type() === expectedType && this.remoteUploadEnabled()) ? this.mainValue() : null);
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
        this.awsAccessKey(dbSettingsDto["Settings"]["Raven/AWSAccessKey"]);
        this.awsSecretKey(dbSettingsDto["SecuredSettings"]["Raven/AWSSecretKey"]);
        this.azureStorageAccount(dbSettingsDto["Settings"]["Raven/AzureStorageAccount"]);
        this.azureStorageKey(dbSettingsDto["SecuredSettings"]["Raven/AzureStorageKey"]);

        if (periodicExportSetup.decryptFailedValue === this.awsSecretKey()) {
            this.awsSecretKey("");
            this.awsSecretKeyDecryptionFailed(true);
        }
        if (periodicExportSetup.decryptFailedValue === this.azureStorageKey()) {
            this.azureStorageKey("");
            this.azureStorageKeyDecryptionFailed(true);
        }
    }

    toDatabaseSettingsDto(): documentDto {
        if (this.additionalAwsInfoRequired()) {
            this.dbSettingsDto["Settings"]["Raven/AWSAccessKey"] = this.awsAccessKey();
            this.dbSettingsDto["SecuredSettings"]["Raven/AWSSecretKey"] = this.awsSecretKey();
        } else {
            delete this.dbSettingsDto["Settings"]["Raven/AWSAccessKey"];
            delete this.dbSettingsDto["SecuredSettings"]["Raven/AWSSecretKey"];
        }
        if (this.additionalAzureInfoRequired()) {
            this.dbSettingsDto["Settings"]["Raven/AzureStorageAccount"] = this.azureStorageAccount();
            this.dbSettingsDto["SecuredSettings"]["Raven/AzureStorageKey"] = this.azureStorageKey();
        } else {
            delete this.dbSettingsDto["Settings"]["Raven/AzureStorageAccount"];
            delete this.dbSettingsDto["SecuredSettings"]["Raven/AzureStorageKey"];
        }
        
        return this.dbSettingsDto;
    }

    removeDatabaseSettings() {
        delete this.dbSettingsDto["Settings"]["Raven/AWSAccessKey"];
        delete this.dbSettingsDto["SecuredSettings"]["Raven/AWSSecretKey"];
        delete this.dbSettingsDto["Settings"]["Raven/AzureStorageAccount"];
        delete this.dbSettingsDto["SecuredSettings"]["Raven/AzureStorageKey"];
        return this.dbSettingsDto;
    }

    getEtag() {
        return this.toDatabaseSettingsDto()["@metadata"]["@etag"];
    }

    setEtag(newEtag) {
        this.toDatabaseSettingsDto()["@metadata"]["@etag"] = newEtag;
    }

    resetDecryptionFailures() {
        this.awsSecretKeyDecryptionFailed(false);
        this.azureStorageKeyDecryptionFailed(false);
    }

    private setupTypeAndMainValue(dto: periodicExportSetupDto) {
        var count = 0;
        if (dto.LocalFolderName) {
            this.localFolderName(dto.LocalFolderName);
            this.onDiskExportEnabled(true);
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
        if (count > 0) {
            this.remoteUploadEnabled(true);
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
        return [0, this.TU_MINUTES];
    }

    private isValidTimeValue(value: number): boolean {
        return value >= 1 && value % 1 === 0;
    }

    copyFrom(from: periodicExportSetup) {
        this.onDiskExportEnabled(from.onDiskExportEnabled());
        this.remoteUploadEnabled(from.remoteUploadEnabled());
        this.localFolderName(from.localFolderName());
        this.unsupported(from.unsupported());
        this.disabled(from.disabled());
        this.type(from.type());
        this.mainValue(from.mainValue());

        this.awsAccessKey(from.awsAccessKey());
        this.awsSecretKey(from.awsSecretKey());
        this.awsRegionEndpoint(from.awsRegionEndpoint());

        this.s3RemoteFolderName(from.s3RemoteFolderName());

        this.azureStorageAccount(from.azureStorageAccount());
        this.azureStorageKey(from.azureStorageKey());
        this.azureRemoteFolderName(from.azureRemoteFolderName());

        this.incrementalBackupInterval(from.incrementalBackupInterval());
        this.incrementalBackupIntervalUnit(from.incrementalBackupIntervalUnit());

        this.fullBackupInterval(from.fullBackupInterval());
        this.fullBackupIntervalUnit(from.fullBackupIntervalUnit());
    }
}

export = periodicExportSetup;