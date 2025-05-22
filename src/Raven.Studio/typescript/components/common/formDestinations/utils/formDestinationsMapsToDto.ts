import {
    FormDestinationData,
    LocalDestination,
    S3Destination,
    AzureDestination,
    GoogleCloudDestination,
    GlacierDestination,
    FtpDestination,
    FormDestinations,
    DestinationsDto,
} from "./formDestinationsTypes";

export function mapBackupSettingsToDto(
    destination: FormDestinationData
): Raven.Client.Documents.Operations.Backups.BackupSettings {
    return {
        Disabled: false,
        GetBackupConfigurationScript: destination.config.isOverrideConfig
            ? {
                  Exec: destination.config.exec,
                  Arguments: destination.config.arguments,
                  TimeoutInMs: destination.config.timeoutInMs,
              }
            : undefined,
    };
}

export function mapLocalToDto(destination: LocalDestination): Raven.Client.Documents.Operations.Backups.LocalSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        FolderPath: destination.folderPath,
    };
}

function mapAmazonToDto(destination: S3Destination | GlacierDestination) {
    return {
        AwsRegionName: destination.awsRegionName,
        AwsAccessKey: destination.awsAccessKey,
        AwsSecretKey: destination.awsSecretKey,
        AwsSessionToken: destination.awsSessionToken,
        RemoteFolderName: destination.remoteFolderName,
    };
}

export function mapS3ToDto(destination: S3Destination): Raven.Client.Documents.Operations.Backups.S3Settings {
    if (!destination.isEnabled) {
        return undefined;
    }

    const customServerUrl =
        !destination.config.isOverrideConfig && destination.isUseCustomHost ? destination.customServerUrl : undefined;

    const forcePathStyle =
        !destination.config.isOverrideConfig && destination.isUseCustomHost ? destination.forcePathStyle : undefined;

    return {
        ...mapBackupSettingsToDto(destination),
        ...mapAmazonToDto(destination),
        CustomServerUrl: customServerUrl,
        ForcePathStyle: forcePathStyle,
        BucketName: destination.bucketName,
    };
}

export function mapAzureToDto(destination: AzureDestination): Raven.Client.Documents.Operations.Backups.AzureSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        StorageContainer: destination.storageContainer,
        RemoteFolderName: destination.remoteFolderName,
        AccountName: destination.accountName,
        AccountKey: destination.accountKey,
        SasToken: destination.sasToken,
    };
}

export function mapGoogleCloudToDto(
    destination: GoogleCloudDestination
): Raven.Client.Documents.Operations.Backups.GoogleCloudSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        BucketName: destination.bucketName,
        RemoteFolderName: destination.remoteFolderName,
        GoogleCredentialsJson: destination.googleCredentialsJson,
    };
}

export function mapGlacierToDto(
    destination: GlacierDestination
): Raven.Client.Documents.Operations.Backups.GlacierSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        ...mapAmazonToDto(destination),
        VaultName: destination.vaultName,
    };
}

export function mapFtpToDto(destination: FtpDestination): Raven.Client.Documents.Operations.Backups.FtpSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    const certificateAsBase64 = destination.url.startsWith("ftps") ? destination.certificateAsBase64 : null;

    return {
        ...mapBackupSettingsToDto(destination),
        Url: destination.url,
        UserName: destination.userName,
        Password: destination.password,
        CertificateAsBase64: certificateAsBase64,
    };
}

export function mapDestinationsToDto(destinations: FormDestinations["destinations"]): DestinationsDto {
    return {
        LocalSettings: mapLocalToDto(destinations.local),
        S3Settings: mapS3ToDto(destinations.s3),
        AzureSettings: mapAzureToDto(destinations.azure),
        GoogleCloudSettings: mapGoogleCloudToDto(destinations.googleCloud),
        GlacierSettings: mapGlacierToDto(destinations.glacier),
        FtpSettings: mapFtpToDto(destinations.ftp),
    };
}
