export interface BackupConfigurationScript {
    isOverrideConfig?: boolean;
    arguments?: string;
    exec?: string;
    timeoutInMs?: number;
}

export interface FormDestinationDataBase {
    isEnabled?: boolean;
    config?: BackupConfigurationScript;
}

export interface LocalDestination extends FormDestinationDataBase {
    folderPath?: string;
}

export interface AmazonDestination {
    awsAccessKey?: string;
    awsRegionName?: string;
    awsSecretKey?: string;
    remoteFolderName?: string;
}

export interface S3Destination extends FormDestinationDataBase, AmazonDestination {
    bucketName?: string;
    customServerUrl?: string;
    forcePathStyle?: boolean;
    isUseCustomHost?: boolean;
}

export interface AzureDestination extends FormDestinationDataBase {
    accountKey?: string;
    accountName?: string;
    remoteFolderName?: string;
    storageContainer?: string;
}

export interface GoogleCloudDestination extends FormDestinationDataBase {
    bucketName?: string;
    googleCredentialsJson?: string;
    remoteFolderName?: string;
}

export interface GlacierDestination extends FormDestinationDataBase, AmazonDestination {
    vaultName?: string;
}

export interface FtpDestination extends FormDestinationDataBase {
    password?: string;
    url?: string;
    userName?: string;
    certificateAsBase64?: string;
}

export interface FormDestinations {
    destinations?: {
        local?: LocalDestination;
        s3?: S3Destination;
        azure?: AzureDestination;
        googleCloud?: GoogleCloudDestination;
        glacier?: GlacierDestination;
        ftp?: FtpDestination;
    };
}

export type FormDestinationData =
    | LocalDestination
    | S3Destination
    | AzureDestination
    | GoogleCloudDestination
    | GlacierDestination
    | FtpDestination;

export interface DestinationsDto {
    AzureSettings: Raven.Client.Documents.Operations.Backups.AzureSettings;
    FtpSettings: Raven.Client.Documents.Operations.Backups.FtpSettings;
    GlacierSettings: Raven.Client.Documents.Operations.Backups.GlacierSettings;
    GoogleCloudSettings: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
    LocalSettings: Raven.Client.Documents.Operations.Backups.LocalSettings;
    S3Settings: Raven.Client.Documents.Operations.Backups.S3Settings;
}
