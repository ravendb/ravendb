import {
    encryptionStepSchema,
    dataDirectoryStepSchema,
} from "components/pages/resources/databases/partials/create/shared/createDatabaseSharedValidation";
import { restorePointSchema } from "components/utils/common";
import * as yup from "yup";

const basicInfoStepSchema = yup.object({
    databaseName: yup
        .string()
        .required()
        .when("$usedDatabaseNames", ([usedDatabaseNames], schema) =>
            schema.notOneOf(usedDatabaseNames, "Database already exists")
        ),
    isSharded: yup.boolean(),
});

function getRestorePointsSchema(sourceType: restoreSource) {
    return yup
        .array()
        .of(
            yup.object({
                nodeTag: yup.string().when(["$sourceType", "$isSharded"], {
                    is: (currentSourceType: restoreSource, isSharded: boolean) =>
                        currentSourceType === sourceType && isSharded,
                    then: (schema) => schema.required(),
                }),
                restorePoint: restorePointSchema.nullable().when("$sourceType", {
                    is: (currentSourceType: restoreSource) => currentSourceType === sourceType,
                    then: (schema) => schema.required(),
                }),
            })
        )
        .min(1);
}

type RestorePoints = yup.InferType<ReturnType<typeof getRestorePointsSchema>>;

function getEncryptionKeySchema(sourceType: restoreSource) {
    return yup.string().when(["$sourceType", "restorePoints"], {
        is: (currentSourceType: restoreSource, restorePoints: RestorePoints) =>
            currentSourceType === sourceType &&
            restorePoints[0]?.restorePoint?.isEncrypted &&
            !restorePoints[0]?.restorePoint?.isSnapshotRestore,
        then: (schema) => schema.base64().required(),
    });
}

const localSource = yup.object({
    directory: yup.string().when("$sourceType", {
        is: "local",
        then: (schema) => schema.required(),
    }),
    restorePoints: getRestorePointsSchema("local"),
    encryptionKey: getEncryptionKeySchema("local"),
});

const ravenCloudSource = yup.object({
    link: yup.string().when("$sourceType", {
        is: "cloud",
        then: (schema) => schema.required(),
    }),
    restorePoints: getRestorePointsSchema("cloud"),
    encryptionKey: getEncryptionKeySchema("cloud"),
    awsSettings: yup
        .object({
            sessionToken: yup.string(),
            accessKey: yup.string(),
            secretKey: yup.string(),
            regionName: yup.string(),
            remoteFolderName: yup.string(),
            bucketName: yup.string(),
            disabled: yup.boolean(),
            getBackupConfigurationScript: yup.string(),
            customServerUrl: yup.string(),
            forcePathStyle: yup.boolean(),
            expireDate: yup.string(),
        })
        .nullable(),
});

const amazonS3Source = yup.object({
    isUseCustomHost: yup.boolean(),
    isForcePathStyle: yup.boolean(),
    customHost: yup.string().when(["isUseCustomHost", "$sourceType"], {
        is: (isUseCustomHost: boolean, sourceType: restoreSource) => isUseCustomHost && sourceType === "amazonS3",
        then: (schema) => schema.required(),
    }),
    accessKey: yup.string().when("$sourceType", {
        is: "amazonS3",
        then: (schema) => schema.required(),
    }),
    secretKey: yup.string().when("$sourceType", {
        is: "amazonS3",
        then: (schema) => schema.required(),
    }),
    awsRegion: yup.string().when(["isUseCustomHost", "$sourceType"], {
        is: (isUseCustomHost: boolean, sourceType: restoreSource) => !isUseCustomHost && sourceType === "amazonS3",
        then: (schema) => schema.required(),
    }),
    bucketName: yup.string().when("$sourceType", {
        is: "amazonS3",
        then: (schema) => schema.required(),
    }),
    remoteFolderName: yup.string(),
    restorePoints: getRestorePointsSchema("amazonS3"),
    encryptionKey: getEncryptionKeySchema("amazonS3"),
});

const azureSource = yup.object({
    accountName: yup.string().when("$sourceType", {
        is: "azure",
        then: (schema) => schema.required(),
    }),
    accountKey: yup.string().when("$sourceType", {
        is: "azure",
        then: (schema) => schema.required(),
    }),
    container: yup.string().when("$sourceType", {
        is: "azure",
        then: (schema) => schema.required(),
    }),
    remoteFolderName: yup.string(),
    restorePoints: getRestorePointsSchema("azure"),
    encryptionKey: getEncryptionKeySchema("azure"),
});

const googleCloudSource = yup.object({
    bucketName: yup.string().when("$sourceType", {
        is: "googleCloud",
        then: (schema) => schema.required(),
    }),
    credentialsJson: yup.string().when("$sourceType", {
        is: "googleCloud",
        then: (schema) => schema.required(),
    }),
    remoteFolderName: yup.string(),
    restorePoints: getRestorePointsSchema("googleCloud"),
    encryptionKey: getEncryptionKeySchema("googleCloud"),
});

const sourceStepSchema = yup.object({
    isDisableOngoingTasksAfterRestore: yup.boolean(),
    isSkipIndexes: yup.boolean(),
    isEncrypted: yup.boolean(),
    sourceType: yup.string<restoreSource>().nullable().required(),
    sourceData: yup.object({
        local: localSource,
        cloud: ravenCloudSource,
        amazonS3: amazonS3Source,
        azure: azureSource,
        googleCloud: googleCloudSource,
    }),
});

export const createDatabaseFromBackupSchema = yup.object({
    basicInfoStep: basicInfoStepSchema,
    sourceStep: sourceStepSchema,
    encryptionStep: encryptionStepSchema,
    dataDirectoryStep: dataDirectoryStepSchema,
});

export type CreateDatabaseFromBackupFormData = yup.InferType<typeof createDatabaseFromBackupSchema>;
