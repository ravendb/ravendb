import { CreateDatabaseFromBackupDto } from "commands/resources/restoreDatabaseFromBackupCommand";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "./createDatabaseFromBackupValidation";
import assertUnreachable from "components/utils/assertUnreachable";
type S3Settings = Raven.Client.Documents.Operations.Backups.S3Settings;
type AzureSettings = Raven.Client.Documents.Operations.Backups.AzureSettings;
type GoogleCloudSettings = Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
type BackupEncryptionSettings = Raven.Client.Documents.Operations.Backups.BackupEncryptionSettings;
type RestoreType = Raven.Client.Documents.Operations.Backups.RestoreType;

const defaultPointsWithTags: FormData["sourceStep"]["sourceData"][RestoreSource]["pointsWithTags"] = [
    {
        restorePoint: null,
        nodeTag: "",
    },
];

const defaultValues: FormData = {
    basicInfoStep: {
        databaseName: "",
        isSharded: false,
    },
    sourceStep: {
        isDisableOngoingTasksAfterRestore: false,
        isSkipIndexes: false,
        isEncrypted: false,
        sourceType: null,
        sourceData: {
            local: {
                directory: "",
                pointsWithTags: defaultPointsWithTags,
            },
            ravenCloud: {
                link: "",
                pointsWithTags: defaultPointsWithTags,
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
                pointsWithTags: defaultPointsWithTags,
            },
            azure: {
                accountKey: "",
                accountName: "",
                container: "",
                remoteFolderName: "",
                pointsWithTags: defaultPointsWithTags,
            },
            googleCloud: {
                bucketName: "",
                credentialsJson: "",
                remoteFolderName: "",
                pointsWithTags: defaultPointsWithTags,
            },
        },
    },
    encryptionStep: {
        isKeySaved: false,
        key: "",
    },
    dataDirectoryStep: {
        isDefault: true,
        directory: "",
    },
};

function getRestoreDtoType(sourceType: RestoreSource): RestoreType {
    switch (sourceType) {
        case "local":
            return "Local";
        case "ravenCloud": // raven cloud stores backups only on S3
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

// TODO maybe refactor to get rid of nesting? this is copy-paste from 5.4
function getEncryptionDto(
    selectedSourceData: FormData["sourceStep"]["sourceData"][RestoreSource],
    encryptionDataIsEncrypted: boolean,
    encryptionDataKey: string
): Pick<CreateDatabaseFromBackupDto, "EncryptionKey" | "BackupEncryptionSettings"> {
    let encryptionSettings: BackupEncryptionSettings = null;
    let databaseEncryptionKey = null;

    const restorePoint = selectedSourceData.pointsWithTags[0].restorePoint;

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
    selectedSourceData: FormData["sourceStep"]["sourceData"][RestoreSource],
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
                selectedSourceData.pointsWithTags.map((pointWithTag, index) => [
                    index,
                    {
                        FolderName: pointWithTag.restorePoint.location,
                        LastFileNameToRestore: pointWithTag.restorePoint.fileName,
                        NodeTag: pointWithTag.nodeTag,
                        ShardNumber: index,
                    },
                ])
            ),
        };
    } else {
        dto["LastFileNameToRestore"] = selectedSourceData.pointsWithTags[0].restorePoint.fileName;
        dto["ShardRestoreSettings"] = null;
    }

    return dto;
}

function getSourceDto(
    sourceStep: FormData["sourceStep"],
    isSharded: boolean,
    encryptionDataIsEncrypted: boolean,
    encryptionDataKey: string
): SelectedSourceDto & Pick<CreateDatabaseFromBackupDto, "BackupLocation" | "Settings"> {
    const backupLocation = isSharded
        ? null
        : sourceStep.sourceData[sourceStep.sourceType].pointsWithTags[0].restorePoint.location;

    switch (sourceStep.sourceType) {
        case "local": {
            const data = sourceStep.sourceData.local;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                BackupLocation: backupLocation,
            };
        }
        case "ravenCloud": {
            const data = sourceStep.sourceData.ravenCloud;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    AwsAccessKey: data.awsSettings.accessKey,
                    AwsSecretKey: data.awsSettings.secretKey,
                    AwsRegionName: data.awsSettings.regionName,
                    BucketName: data.awsSettings.bucketName,
                    AwsSessionToken: data.awsSettings.sessionToken,
                    RemoteFolderName: backupLocation,
                    Disabled: false,
                    CustomServerUrl: null,
                    ForcePathStyle: false,
                    GetBackupConfigurationScript: null,
                } satisfies S3Settings,
            };
        }
        case "amazonS3": {
            const data = sourceStep.sourceData.amazonS3;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    AwsAccessKey: data.accessKey,
                    AwsSecretKey: data.secretKey,
                    AwsRegionName: data.awsRegion,
                    BucketName: data.bucketName,
                    AwsSessionToken: "",
                    RemoteFolderName: backupLocation,
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                    CustomServerUrl: data.isUseCustomHost ? data.customHost : null,
                    ForcePathStyle: data.isUseCustomHost && data.isForcePathStyle,
                } satisfies S3Settings,
            };
        }
        case "azure": {
            const data = sourceStep.sourceData.azure;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    AccountKey: data.accountKey,
                    SasToken: "",
                    AccountName: data.accountName,
                    StorageContainer: data.container,
                    RemoteFolderName: backupLocation,
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                } satisfies AzureSettings,
            };
        }

        case "googleCloud": {
            const data = sourceStep.sourceData.googleCloud;

            return {
                ...getSelectedSourceDto(isSharded, data, encryptionDataIsEncrypted, encryptionDataKey),
                Settings: {
                    BucketName: data.bucketName,
                    GoogleCredentialsJson: data.credentialsJson,
                    RemoteFolderName: backupLocation,
                    Disabled: false,
                    GetBackupConfigurationScript: null,
                } satisfies GoogleCloudSettings,
            };
        }
        default:
            assertUnreachable(sourceStep.sourceType);
    }
}

function mapToDto({
    basicInfoStep,
    sourceStep,
    encryptionStep,
    dataDirectoryStep,
}: FormData): CreateDatabaseFromBackupDto {
    return {
        ...getSourceDto(sourceStep, basicInfoStep.isSharded, sourceStep.isEncrypted, encryptionStep.key),
        Type: getRestoreDtoType(sourceStep.sourceType),
        DatabaseName: basicInfoStep.databaseName,
        DisableOngoingTasks: sourceStep.isDisableOngoingTasksAfterRestore,
        SkipIndexes: sourceStep.isSkipIndexes,
        DataDirectory: dataDirectoryStep.isDefault ? null : dataDirectoryStep.directory,
    };
}

export const createDatabaseFromBackupDataUtils = {
    defaultValues,
    mapToDto,
};
