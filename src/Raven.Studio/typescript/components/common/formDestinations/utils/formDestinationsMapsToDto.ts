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
        GetBackupConfigurationScript: destination.isOverrideConfig
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

export function mapS3ToDto(destination: S3Destination): Raven.Client.Documents.Operations.Backups.S3Settings {
    if (!destination.isEnabled) {
        return undefined;
    }

    const customServerUrl =
        !destination.isOverrideConfig && destination.isUseCustomHost ? destination.customServerUrl : undefined;

    const forcePathStyle =
        !destination.isOverrideConfig && destination.isUseCustomHost ? destination.forcePathStyle : undefined;

    return {
        ...mapBackupSettingsToDto(destination),
        CustomServerUrl: customServerUrl,
        ForcePathStyle: forcePathStyle,
        BucketName: destination.bucketName,
        RemoteFolderName: destination.remoteFolderName,
        AwsRegionName: destination.awsRegionName,
        AwsAccessKey: destination.awsAccessKey,
        AwsSecretKey: destination.awsSecretKey,
        AwsSessionToken: undefined, // TODO
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
        SasToken: "", // TODO
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
        VaultName: destination.vaultName,
        RemoteFolderName: destination.remoteFolderName,
        AwsRegionName: destination.awsRegionName,
        AwsAccessKey: destination.awsAccessKey,
        AwsSecretKey: destination.awsSecretKey,
        AwsSessionToken: undefined, // TODO
    };
}

export function mapFtpToDto(destination: FtpDestination): Raven.Client.Documents.Operations.Backups.FtpSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        Url: destination.url,
        UserName: destination.userName,
        Password: destination.password,
        CertificateAsBase64: destination.certificateAsBase64,
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
