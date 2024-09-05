import {
    encryptionStepSchema,
    dataDirectoryStepSchema,
} from "components/pages/resources/databases/partials/create/shared/createDatabaseSharedValidation";
import * as yup from "yup";

const basicInfoStepSchema = yup.object({
    databaseName: yup
        .string()
        .trim()
        .strict()
        .required()
        .when("$usedDatabaseNames", ([usedDatabaseNames], schema) =>
            schema.notOneOf(usedDatabaseNames, "Database already exists")
        ),
    isSharded: yup.boolean(),
});

export type RestoreSource = "local" | "ravenCloud" | "amazonS3" | "azure" | "googleCloud";

function getPointsWithTagsSchema(sourceType: RestoreSource) {
    return yup
        .array()
        .of(
            yup.object({
                nodeTag: yup.string().when(["$sourceType", "$isSharded"], {
                    is: (currentSourceType: RestoreSource, isSharded: boolean) =>
                        currentSourceType === sourceType && isSharded,
                    then: (schema) => schema.required(),
                }),
                restorePoint: restorePointSchema.nullable().when("$sourceType", {
                    is: (currentSourceType: RestoreSource) => currentSourceType === sourceType,
                    then: (schema) => schema.required(),
                }),
            })
        )
        .when(["$sourceType", "$isSharded"], {
            is: (currentSourceType: RestoreSource, isSharded: boolean) => currentSourceType === sourceType && isSharded,
            then: (schema) =>
                schema.test("one-must-be-local", "At least one selected node must be local", (value, ctx) => {
                    return value.some((x) => x.nodeTag === ctx.options.context.localNodeTag);
                }),
        })
        .min(1);
}

export const restorePointSchema = yup.object({
    dateTime: yup.string().required(),
    location: yup.string().required(),
    fileName: yup.string().required(),
    isSnapshotRestore: yup.boolean().required(),
    isIncremental: yup.boolean().required(),
    isEncrypted: yup.boolean().required(),
    filesToRestore: yup.number().required(),
    databaseName: yup.string().nullable(),
    nodeTag: yup.string().nullable(),
    backupType: yup.string().required(),
});

export type RestorePoint = yup.InferType<typeof restorePointSchema>;

type PointsWithTags = yup.InferType<ReturnType<typeof getPointsWithTagsSchema>>;

function getEncryptionKeySchema(sourceType: RestoreSource) {
    return yup.string().when(["$sourceType", "pointsWithTags"], {
        is: (currentSourceType: RestoreSource, pointsWithTags: PointsWithTags) =>
            currentSourceType === sourceType && pointsWithTags[0]?.restorePoint?.isEncrypted,
        then: (schema) => schema.base64().required(),
    });
}

const localSource = yup.object({
    directory: yup.string().when("$sourceType", {
        is: "local",
        then: (schema) => schema.required(),
    }),
    pointsWithTags: getPointsWithTagsSchema("local"),
    encryptionKey: getEncryptionKeySchema("local"),
});

const ravenCloudSource = yup.object({
    link: yup.string().when("$sourceType", {
        is: "ravenCloud",
        then: (schema) => schema.url().required(),
    }),
    pointsWithTags: getPointsWithTagsSchema("ravenCloud"),
    encryptionKey: getEncryptionKeySchema("ravenCloud"),
    awsSettings: yup
        .object({
            sessionToken: yup.string().nullable(),
            accessKey: yup.string().nullable(),
            secretKey: yup.string().nullable(),
            regionName: yup.string().nullable(),
            remoteFolderName: yup.string().nullable(),
            bucketName: yup.string().nullable(),
            expireDate: yup.string().nullable(),
        })
        .nullable(),
});

const amazonS3Source = yup.object({
    isUseCustomHost: yup.boolean(),
    isForcePathStyle: yup.boolean(),
    customHost: yup.string().when(["isUseCustomHost", "$sourceType"], {
        is: (isUseCustomHost: boolean, sourceType: RestoreSource) => isUseCustomHost && sourceType === "amazonS3",
        then: (schema) => schema.trim().strict().required(),
    }),
    accessKey: yup.string().when("$sourceType", {
        is: "amazonS3",
        then: (schema) => schema.trim().strict().required(),
    }),
    secretKey: yup.string().when("$sourceType", {
        is: "amazonS3",
        then: (schema) => schema.trim().strict().required(),
    }),
    awsRegion: yup.string().when(["isUseCustomHost", "$sourceType"], {
        is: (isUseCustomHost: boolean, sourceType: RestoreSource) => !isUseCustomHost && sourceType === "amazonS3",
        then: (schema) => schema.trim().strict().required(),
    }),
    bucketName: yup.string().when("$sourceType", {
        is: "amazonS3",
        then: (schema) => schema.trim().strict().required(),
    }),
    remoteFolderName: yup.string().trim().strict(),
    pointsWithTags: getPointsWithTagsSchema("amazonS3"),
    encryptionKey: getEncryptionKeySchema("amazonS3"),
});

const azureSource = yup.object({
    accountName: yup.string().when("$sourceType", {
        is: "azure",
        then: (schema) => schema.trim().strict().required(),
    }),
    accountKey: yup.string().when("$sourceType", {
        is: "azure",
        then: (schema) => schema.trim().strict().required(),
    }),
    container: yup.string().when("$sourceType", {
        is: "azure",
        then: (schema) => schema.trim().strict().required(),
    }),
    remoteFolderName: yup.string().trim().strict(),
    pointsWithTags: getPointsWithTagsSchema("azure"),
    encryptionKey: getEncryptionKeySchema("azure"),
});

const googleCloudSource = yup.object({
    bucketName: yup.string().when("$sourceType", {
        is: "googleCloud",
        then: (schema) => schema.trim().strict().required(),
    }),
    credentialsJson: yup.string().when("$sourceType", {
        is: "googleCloud",
        then: (schema) => schema.required(),
    }),
    remoteFolderName: yup.string().trim().strict(),
    pointsWithTags: getPointsWithTagsSchema("googleCloud"),
    encryptionKey: getEncryptionKeySchema("googleCloud"),
});

const sourceStepSchema = yup.object({
    isDisableOngoingTasksAfterRestore: yup.boolean(),
    isSkipIndexes: yup.boolean(),
    isEncrypted: yup.boolean(),
    sourceType: yup.string<RestoreSource>().nullable().required(),
    sourceData: yup.object({
        local: localSource,
        ravenCloud: ravenCloudSource,
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
