import { yupObjectSchema } from "components/utils/yupUtils";
import * as yup from "yup";

export interface BackupConfigurationScript {
    arguments: string;
    exec: string;
    timeoutInMs: number;
}

interface DestinationFormBase {
    isEnabled: boolean;
    isOverrideConfig: boolean;
    config?: BackupConfigurationScript;
}

interface LocalDestination extends DestinationFormBase {
    folderPath: string;
}

interface AmazonDestination {
    awsAccessKey: string;
    awsRegionName: string;
    awsSecretKey: string;
    remoteFolderName: string;
}

interface S3Destination extends DestinationFormBase, AmazonDestination {
    bucketName: string;
    customServerUrl: string;
    forcePathStyle: boolean;
    isUseCustomHost: boolean;
}

interface AzureDestination extends DestinationFormBase {
    accountKey: string;
    accountName: string;
    remoteFolderName: string;
    storageContainer: string;
}

interface GoogleCloudDestination extends DestinationFormBase {
    bucketName: string;
    googleCredentialsJson: string;
    remoteFolderName: string;
}

interface GlacierDestination extends DestinationFormBase, AmazonDestination {
    vaultName: string;
}

interface FtpDestination extends DestinationFormBase {
    password: string;
    url: string;
    userName: string;
    certificateAsBase64: string;
}

export interface FormDestinations {
    local?: LocalDestination;
    s3?: S3Destination;
    azure?: AzureDestination;
    googleCloud?: GoogleCloudDestination;
    glacier?: GlacierDestination;
    ftp?: FtpDestination;
}

export type Destination =
    | LocalDestination
    | S3Destination
    | AzureDestination
    | GoogleCloudDestination
    | GlacierDestination
    | FtpDestination;

const configSchema = yupObjectSchema<BackupConfigurationScript>({
    arguments: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    exec: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    timeoutInMs: yup
        .number()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const destinationBaseSchema = yupObjectSchema<DestinationFormBase>({
    isEnabled: yup.boolean(),
    isOverrideConfig: yup.boolean(),
    config: configSchema,
});

type WithoutBase<T extends DestinationFormBase> = Omit<T, "isEnabled" | "isOverrideConfig" | "config">;
type WithoutAmazonAndBase<T extends S3Destination | GlacierDestination> = Omit<
    WithoutBase<T>,
    "awsAccessKey" | "awsRegionName" | "awsSecretKey" | "remoteFolderName"
>;

const localSchema = yupObjectSchema<WithoutBase<LocalDestination>>({
    folderPath: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

const amazonSchema = yupObjectSchema<AmazonDestination>({
    remoteFolderName: yup.string().nullable(),
    awsRegionName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    awsAccessKey: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    awsSecretKey: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const s3Schema = yupObjectSchema<WithoutAmazonAndBase<S3Destination>>({
    bucketName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    forcePathStyle: yup.boolean(),
    customServerUrl: yup.string().nullable(),
    isUseCustomHost: yup.boolean(),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const azureSchema = yupObjectSchema<WithoutBase<AzureDestination>>({
    storageContainer: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    remoteFolderName: yup.string().nullable(),
    accountName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    accountKey: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

const googleCloudSchema = yupObjectSchema<WithoutBase<GoogleCloudDestination>>({
    bucketName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    remoteFolderName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    googleCredentialsJson: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

const glacierSchema = yupObjectSchema<WithoutAmazonAndBase<GlacierDestination>>({
    vaultName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const ftpSchema = yupObjectSchema<WithoutBase<Omit<FtpDestination, "certificateAsBase64">>>({
    url: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    userName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    password: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

export const destinationsSchema = yupObjectSchema<FormDestinations>({
    local: localSchema,
    s3: s3Schema,
    azure: azureSchema,
    googleCloud: googleCloudSchema,
    glacier: glacierSchema,
    ftp: ftpSchema,
});

function mapBackupSettingsToDto(destination: Destination): Raven.Client.Documents.Operations.Backups.BackupSettings {
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

interface DestinationsDto {
    AzureSettings: Raven.Client.Documents.Operations.Backups.AzureSettings;
    FtpSettings: Raven.Client.Documents.Operations.Backups.FtpSettings;
    GlacierSettings: Raven.Client.Documents.Operations.Backups.GlacierSettings;
    GoogleCloudSettings: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
    LocalSettings: Raven.Client.Documents.Operations.Backups.LocalSettings;
    S3Settings: Raven.Client.Documents.Operations.Backups.S3Settings;
}

function mapLocalToDto(destination: LocalDestination): Raven.Client.Documents.Operations.Backups.LocalSettings {
    if (!destination.isEnabled) {
        return undefined;
    }

    return {
        ...mapBackupSettingsToDto(destination),
        FolderPath: destination.folderPath,
    };
}

function mapS3ToDto(destination: S3Destination): Raven.Client.Documents.Operations.Backups.S3Settings {
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

function mapAzureToDto(destination: AzureDestination): Raven.Client.Documents.Operations.Backups.AzureSettings {
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

function mapGoogleCloudToDto(
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

function mapGlacierToDto(destination: GlacierDestination): Raven.Client.Documents.Operations.Backups.GlacierSettings {
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

function mapFtpToDto(destination: FtpDestination): Raven.Client.Documents.Operations.Backups.FtpSettings {
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

export function mapDestinationsToDto<T extends FormDestinations>(destinations: T): DestinationsDto {
    return {
        LocalSettings: mapLocalToDto(destinations.local),
        S3Settings: mapS3ToDto(destinations.s3),
        AzureSettings: mapAzureToDto(destinations.azure),
        GoogleCloudSettings: mapGoogleCloudToDto(destinations.googleCloud),
        GlacierSettings: mapGlacierToDto(destinations.glacier),
        FtpSettings: mapFtpToDto(destinations.ftp),
    };
}

const defaultConfig: BackupConfigurationScript = {
    exec: null,
    arguments: null,
    timeoutInMs: null,
};

const defaultFormBase: DestinationFormBase = {
    isEnabled: false,
    isOverrideConfig: false,
    config: defaultConfig,
};

function mapFormBaseFromDto(dto: Raven.Client.Documents.Operations.Backups.BackupSettings): DestinationFormBase {
    return {
        isEnabled: !dto.Disabled,
        isOverrideConfig: dto.GetBackupConfigurationScript != null,
        config:
            dto.GetBackupConfigurationScript != null
                ? {
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

export const defaultS3FormData: S3Destination = {
    ...defaultFormBase,
    isUseCustomHost: false,
    customServerUrl: null,
    forcePathStyle: false,
    bucketName: null,
    remoteFolderName: null,
    awsRegionName: null,
    awsAccessKey: null,
    awsSecretKey: null,
};

function mapS3FromDto(dto: Raven.Client.Documents.Operations.Backups.S3Settings): S3Destination {
    if (!dto) {
        return defaultS3FormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        isUseCustomHost: dto.CustomServerUrl != null,
        customServerUrl: dto.CustomServerUrl,
        forcePathStyle: dto.ForcePathStyle,
        bucketName: dto.BucketName,
        remoteFolderName: dto.RemoteFolderName,
        awsRegionName: dto.AwsRegionName,
        awsAccessKey: dto.AwsAccessKey,
        awsSecretKey: dto.AwsAccessKey,
    };
}

export const defaultAzureFormData: AzureDestination = {
    ...defaultFormBase,
    storageContainer: null,
    remoteFolderName: null,
    accountName: null,
    accountKey: null,
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
    vaultName: null,
    remoteFolderName: null,
    awsRegionName: null,
    awsAccessKey: null,
    awsSecretKey: null,
};

function mapGlacierCloudFromDto(dto: Raven.Client.Documents.Operations.Backups.GlacierSettings): GlacierDestination {
    if (!dto) {
        return defaultGlacierFormData;
    }

    return {
        ...mapFormBaseFromDto(dto),
        vaultName: dto.VaultName,
        remoteFolderName: dto.RemoteFolderName,
        awsRegionName: dto.AwsRegionName,
        awsAccessKey: dto.AwsAccessKey,
        awsSecretKey: dto.AwsAccessKey,
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
        local: mapLocalFromDto(dto.LocalSettings),
        s3: mapS3FromDto(dto.S3Settings),
        azure: mapAzureFromDto(dto.AzureSettings),
        googleCloud: mapGoogleCloudFromDto(dto.GoogleCloudSettings),
        glacier: mapGlacierCloudFromDto(dto.GlacierSettings),
        ftp: mapFtpFromDto(dto.FtpSettings),
    };
}
