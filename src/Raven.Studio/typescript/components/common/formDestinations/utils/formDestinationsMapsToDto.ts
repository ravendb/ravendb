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
import { getFtpProtocol } from "./ftpUrlUtils";

function trim(value: string): string {
    return value?.trim() ?? value;
}

function trimToNull(value: string): string | null {
    return value?.trim() || null;
}

export function mapBackupSettingsToDto(
    destination: FormDestinationData
): Raven.Client.Documents.Operations.Backups.BackupSettings {
    return {
        Disabled: false,
        GetBackupConfigurationScript: destination.config.isOverrideConfig
            ? {
                  Exec: destination.config.exec,
                  Arguments: destination.config.arguments,
                  TimeoutInMs: destination.config.timeoutInMs ?? undefined,
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
        FolderPath: trim(destination.folderPath),
    };
}

export function mapAmazonToDto(destination: S3Destination | GlacierDestination) {
    return {
        AwsRegionName: trim(destination.awsRegionName),
        AwsAccessKey: trim(destination.awsAccessKey),
        AwsSecretKey: trim(destination.awsSecretKey),
        AwsSessionToken: trimToNull(destination.awsSessionToken),
        RemoteFolderName: trimToNull(destination.remoteFolderName),
    };
}

export function mapS3ToDto(destination: S3Destination): Raven.Client.Documents.Operations.Backups.S3Settings {
    if (!destination.isEnabled) {
        return undefined;
    }

    const customServerUrl =
        !destination.config.isOverrideConfig && destination.isUseCustomHost
            ? trim(destination.customServerUrl)
            : undefined;

    const forcePathStyle =
        !destination.config.isOverrideConfig && destination.isUseCustomHost ? destination.forcePathStyle : undefined;

    const disableChecksumValidation =
        !destination.config.isOverrideConfig && destination.isUseCustomHost
            ? destination.disableChecksumValidation
            : undefined;

    return {
        ...mapBackupSettingsToDto(destination),
        ...mapAmazonToDto(destination),
        CustomServerUrl: customServerUrl,
        ForcePathStyle: forcePathStyle,
        DisableChecksumValidation: disableChecksumValidation,
        BucketName: trim(destination.bucketName),
        StorageClass: destination.storageClass ?? undefined,
    };
}

export function mapAzureCredentialsToDto(
    destination: Pick<AzureDestination, "authType" | "accountKey" | "sasToken">
): Pick<Raven.Client.Documents.Operations.Backups.AzureSettings, "AccountKey" | "SasToken"> {
    const isSasToken = destination.authType === "sasToken";

    return {
        AccountKey: isSasToken ? null : trim(destination.accountKey),
        SasToken: isSasToken ? trim(destination.sasToken) : null,
    };
}

export function mapAzureToDto(destination: AzureDestination): Raven.Client.Documents.Operations.Backups.AzureSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        ...mapAzureCredentialsToDto(destination),
        StorageContainer: trim(destination.storageContainer),
        RemoteFolderName: trimToNull(destination.remoteFolderName),
        AccountName: trim(destination.accountName),
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
        BucketName: trim(destination.bucketName),
        RemoteFolderName: trimToNull(destination.remoteFolderName),
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
        VaultName: trim(destination.vaultName),
    };
}

export function mapFtpToDto(destination: FtpDestination): Raven.Client.Documents.Operations.Backups.FtpSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    const certificateAsBase64 = getFtpProtocol(destination.url) === "ftps" ? destination.certificateAsBase64 : null;

    return {
        ...mapBackupSettingsToDto(destination),
        Url: trim(destination.url),
        UserName: trim(destination.userName),
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
