import { CreateDatabaseFromBackupFormData as FormData } from "./createDatabaseFromBackupValidation";
import assertUnreachable from "components/utils/assertUnreachable";
type RestoreBackupConfigurationBase = Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;
type S3Settings = Raven.Client.Documents.Operations.Backups.S3Settings;
type AzureSettings = Raven.Client.Documents.Operations.Backups.AzureSettings;
type GoogleCloudSettings = Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
type BackupEncryptionSettings = Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings;
type RestoreType = Raven.Client.Documents.Operations.Backups.RestoreType;

const defaultRestorePoints: FormData["source"]["sourceData"][restoreSource]["restorePoints"] = [
    {
        restorePoint: null,
        nodeTag: "",
    },
];

const defaultValues: FormData = {
    basicInfo: {
        databaseName: "",
        isSharded: false,
    },
    source: {
        isDisableOngoingTasksAfterRestore: false,
        isSkipIndexes: false,
        isEncrypted: false,
        sourceType: null,
        sourceData: {
            local: {
                directory: "",
                restorePoints: defaultRestorePoints,
            },
            cloud: {
                link: "",
                restorePoints: defaultRestorePoints,
                encryptionKey: "",
                awsSettings: null,
            },
            amazonS3: {
                isUseCustomHost: false,
                isForcePathStyle: false,
                customHost: "",
                accessKey: "",
                secretKey: "",
                awsRegion: "",
                bucketName: "",
                remoteFolderName: "",
                restorePoints: defaultRestorePoints,
            },
            azure: {
                accountKey: "",
                accountName: "",
                container: "",
                remoteFolderName: "",
                restorePoints: defaultRestorePoints,
            },
            googleCloud: {
                bucketName: "",
                credentialsJson: "",
                remoteFolderName: "",
                restorePoints: defaultRestorePoints,
            },
        },
    },
    encryption: {
        isKeySaved: false,
        key: "",
    },
    pathsConfigurations: {
        isDefault: true,
        path: "",
    },
};

// TODO rename path -> dataDirectory?
function getRestoreDtoType(sourceType: restoreSource): RestoreType {
    switch (sourceType) {
        case "local":
            return "Local";
        case "cloud": // raven cloud stores backups only on S3
        case "amazonS3":
            return "S3";
        case "azure":
            return "Azure";
        case "googleCloud":
            return "GoogleCloud";
        default:
            assertUnreachable(sourceType);
    }
}

// TODO refactor
function getEncryptionDto(
    selectedSourceData: FormData["source"]["sourceData"][restoreSource],
    encryptionDataIsEncrypted: boolean,
    encryptionDataKey: string
): Pick<RestoreBackupConfigurationBase, "EncryptionKey" | "BackupEncryptionSettings"> {
    let encryptionSettings: BackupEncryptionSettings = null;
    let databaseEncryptionKey = null;

    const restorePoint = selectedSourceData.restorePoints[0].restorePoint;

    if (restorePoint.isEncrypted) {
        if (restorePoint.isSnapshotRestore) {
            if (encryptionDataIsEncrypted) {
                encryptionSettings = {
                    EncryptionMode: "UseDatabaseKey",
                    Key: null,
                };
                databaseEncryptionKey = selectedSourceData.encryptionKey;
            }
        } else {
            // backup of type backup
            encryptionSettings = {
                EncryptionMode: "UseProvidedKey",
                Key: selectedSourceData.encryptionKey,
            };

            if (encryptionDataIsEncrypted) {
                databaseEncryptionKey = encryptionDataKey;
            }
        }
    } else {
        // backup is not encrypted
        if (!restorePoint.isSnapshotRestore && encryptionDataIsEncrypted) {
            databaseEncryptionKey = encryptionDataKey;
        }
    }

    return {
        BackupEncryptionSettings: encryptionSettings,
        EncryptionKey: databaseEncryptionKey,
    };
}

type SelectedSourceDto = Pick<
    CreateDatabaseFromBackupDto,
    "LastFileNameToRestore" | "ShardRestoreSettings" | "BackupEncryptionSettings" | "EncryptionKey"
>;

function getSelectedSourceDto(
    isSharded: boolean,
    selectedSourceData: FormData["source"]["sourceData"][restoreSource],
    encryptionDataIsEncrypted: boolean,
    encryptionDataKey: string
): SelectedSourceDto {
    const encryptionDto = getEncryptionDto(selectedSourceData, encryptionDataIsEncrypted, encryptionDataKey);

    const dto: SelectedSourceDto = {
        ...encryptionDto,
    };

    if (isSharded) {
        dto["LastFileNameToRestore"] = null;
        dto["ShardRestoreSettings"] = {
            Shards: Object.fromEntries(
                selectedSourceData.restorePoints.map((restorePoint, index) => [
                    index,
                    {
                        FolderName: restorePoint.restorePoint.location,
                        LastFileNameToRestore: restorePoint.restorePoint.fileName,
                        NodeTag: restorePoint.nodeTag,
                        ShardNumber: index,
                    },
                ])
            ),
        };
    } else {
        dto["LastFileNameToRestore"] = selectedSourceData.restorePoints[0].restorePoint.fileName;
        dto["ShardRestoreSettings"] = null;
    }

    return dto;
}

function getSourceDto(
    source: FormData["source"],
    isSharded: boolean,
    encryptionDataIsEncrypted: boolean,
    encryptionDataKey: string
): SelectedSourceDto & Pick<CreateDatabaseFromBackupDto, "BackupLocation" | "Settings"> {
    switch (source.sourceType) {
        case "local": {
            const data = source.sourceData.local;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                BackupLocation: isSharded ? null : data.restorePoints[0].restorePoint.location,
            };
        }
        case "cloud": {
            const data = source.sourceData.cloud;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    AwsAccessKey: _.trim(data.awsSettings.accessKey),
                    AwsSecretKey: _.trim(data.awsSettings.secretKey),
                    AwsRegionName: _.trim(data.awsSettings.regionName),
                    BucketName: _.trim(data.awsSettings.bucketName),
                    AwsSessionToken: "",
                    RemoteFolderName: _.trim(data.awsSettings.remoteFolderName),
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    CustomServerUrl: _.trim(data.awsSettings.customServerUrl),
                    ForcePathStyle: data.awsSettings.forcePathStyle,
                } satisfies S3Settings,
            };
        }
        case "amazonS3": {
            const data = source.sourceData.amazonS3;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    AwsAccessKey: _.trim(data.accessKey),
                    AwsSecretKey: _.trim(data.secretKey),
                    AwsRegionName: _.trim(data.awsRegion),
                    BucketName: _.trim(data.bucketName),
                    AwsSessionToken: "",
                    RemoteFolderName: _.trim(data.remoteFolderName),
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    CustomServerUrl: data.isUseCustomHost ? _.trim(data.customHost) : null,
                    ForcePathStyle: data.isUseCustomHost && data.isForcePathStyle,
                } satisfies S3Settings,
            };
        }
        case "azure": {
            const data = source.sourceData.azure;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    AccountKey: _.trim(data.accountKey),
                    SasToken: "",
                    AccountName: _.trim(data.accountName),
                    StorageContainer: _.trim(data.container),
                    RemoteFolderName: _.trim(data.remoteFolderName),
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                } satisfies AzureSettings,
            };
        }

        case "googleCloud": {
            const data = source.sourceData.googleCloud;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    BucketName: _.trim(data.bucketName),
                    GoogleCredentialsJson: _.trim(data.credentialsJson),
                    RemoteFolderName: _.trim(data.remoteFolderName),
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                } satisfies GoogleCloudSettings,
            };
        }
        default:
            assertUnreachable(source.sourceType);
    }
}

export type CreateDatabaseFromBackupDto = Partial<RestoreBackupConfigurationBase> & {
    Type: RestoreType;
} & {
    BackupLocation?: string;
    Settings?: S3Settings | AzureSettings | GoogleCloudSettings;
};

function mapToDto({ basicInfo, source, encryption, pathsConfigurations }: FormData): CreateDatabaseFromBackupDto {
    return {
        ...getSourceDto(source, basicInfo.isSharded, source.isEncrypted, encryption.key),
        Type: getRestoreDtoType(source.sourceType),
        DatabaseName: basicInfo.databaseName,
        DisableOngoingTasks: source.isDisableOngoingTasksAfterRestore,
        SkipIndexes: source.isSkipIndexes,
        DataDirectory: pathsConfigurations.isDefault ? null : _.trim(pathsConfigurations.path),
    };
}

export const createDatabaseFromBackupDataUtils = {
    defaultValues,
    mapToDto,
};
