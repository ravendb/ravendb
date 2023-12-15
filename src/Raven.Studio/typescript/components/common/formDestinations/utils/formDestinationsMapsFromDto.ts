import {
    BackupConfigurationScript,
    FormDestinationDataBase,
    LocalDestination,
    S3Destination,
    AzureDestination,
    GoogleCloudDestination,
    GlacierDestination,
    FtpDestination,
    DestinationsDto,
    FormDestinations,
    AmazonDestination,
} from "./formDestinationsTypes";

const defaultConfig: BackupConfigurationScript = {
    isOverrideConfig: false,
    exec: null,
    arguments: null,
    timeoutInMs: null,
};

const defaultFormBase: FormDestinationDataBase = {
    isEnabled: false,
    config: defaultConfig,
};

function mapFormBaseFromDto(dto: Raven.Client.Documents.Operations.Backups.BackupSettings): FormDestinationDataBase {
    return {
        isEnabled: !dto.Disabled,
        config:
            dto.GetBackupConfigurationScript != null
                ? {
                      isOverrideConfig: true,
                      exec: dto.GetBackupConfigurationScript.Exec,
                      arguments: dto.GetBackupConfigurationScript.Arguments,
                      timeoutInMs: dto.GetBackupConfigurationScript.TimeoutInMs,
                  }
                : defaultConfig,
    };
}

export const defaultLocalFormData: LocalDestination = {
    ...defaultFormBase,
    folderPath: null,
};

function mapLocalFromDto(dto: Raven.Client.Documents.Operations.Backups.LocalSettings): LocalDestination {
    if (!dto) {
        return defaultLocalFormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        folderPath: dto.FolderPath,
    };
}

export const defaultAmazonFormData: AmazonDestination = {
    awsRegionName: null,
    awsAccessKey: null,
    awsSecretKey: null,
    awsSessionToken: null,
    remoteFolderName: null,
};

function mapAmazonFromDto(
    dto:
        | Raven.Client.Documents.Operations.Backups.S3Settings
        | Raven.Client.Documents.Operations.Backups.GlacierSettings
): AmazonDestination {
    return {
        awsRegionName: dto.AwsRegionName,
        awsAccessKey: dto.AwsAccessKey,
        awsSecretKey: dto.AwsSecretKey,
        awsSessionToken: dto.AwsSessionToken,
        remoteFolderName: dto.RemoteFolderName,
    };
}

export const defaultS3FormData: S3Destination = {
    ...defaultFormBase,
    ...defaultAmazonFormData,
    isUseCustomHost: false,
    customServerUrl: null,
    forcePathStyle: false,
    bucketName: null,
};

function mapS3FromDto(dto: Raven.Client.Documents.Operations.Backups.S3Settings): S3Destination {
    if (!dto) {
        return defaultS3FormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        ...mapAmazonFromDto(dto),
        isUseCustomHost: dto.CustomServerUrl != null,
        customServerUrl: dto.CustomServerUrl,
        forcePathStyle: dto.ForcePathStyle,
        bucketName: dto.BucketName,
    };
}

export const defaultAzureFormData: AzureDestination = {
    ...defaultFormBase,
    storageContainer: null,
    remoteFolderName: null,
    accountName: null,
    accountKey: null,
    sasToken: null,
};

function mapAzureFromDto(dto: Raven.Client.Documents.Operations.Backups.AzureSettings): AzureDestination {
    if (!dto) {
        return defaultAzureFormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        storageContainer: dto.StorageContainer,
        remoteFolderName: dto.RemoteFolderName,
        accountName: dto.AccountName,
        accountKey: dto.AccountKey,
        sasToken: dto.SasToken,
    };
}

export const defaultGoogleCloudFormData: GoogleCloudDestination = {
    ...defaultFormBase,
    bucketName: null,
    remoteFolderName: null,
    googleCredentialsJson: null,
};

function mapGoogleCloudFromDto(
    dto: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings
): GoogleCloudDestination {
    if (!dto) {
        return defaultGoogleCloudFormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        bucketName: dto.BucketName,
        remoteFolderName: dto.RemoteFolderName,
        googleCredentialsJson: dto.GoogleCredentialsJson,
    };
}

export const defaultGlacierFormData: GlacierDestination = {
    ...defaultFormBase,
    ...defaultAmazonFormData,
    vaultName: null,
};

function mapGlacierCloudFromDto(dto: Raven.Client.Documents.Operations.Backups.GlacierSettings): GlacierDestination {
    if (!dto) {
        return defaultGlacierFormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        ...mapAmazonFromDto(dto),
        vaultName: dto.VaultName,
    };
}

export const defaultFtpFormData: FtpDestination = {
    ...defaultFormBase,
    url: null,
    userName: null,
    password: null,
    certificateAsBase64: null,
};

function mapFtpFromDto(dto: Raven.Client.Documents.Operations.Backups.FtpSettings): FtpDestination {
    if (!dto) {
        return defaultFtpFormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        url: dto.Url,
        userName: dto.UserName,
        password: dto.Password,
        certificateAsBase64: dto.CertificateAsBase64,
    };
}

export function mapDestinationsFromDto(dto: DestinationsDto): FormDestinations {
    return {
        destinations: {
            local: mapLocalFromDto(dto.LocalSettings),
            s3: mapS3FromDto(dto.S3Settings),
            azure: mapAzureFromDto(dto.AzureSettings),
            googleCloud: mapGoogleCloudFromDto(dto.GoogleCloudSettings),
            glacier: mapGlacierCloudFromDto(dto.GlacierSettings),
            ftp: mapFtpFromDto(dto.FtpSettings),
        },
    };
}
