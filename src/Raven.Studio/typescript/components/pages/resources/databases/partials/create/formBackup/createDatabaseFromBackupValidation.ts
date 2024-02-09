import {
    encryptionSchema,
    pathsConfigurationsSchema,
} from "components/pages/resources/databases/partials/create/shared/createDatabaseSharedValidation";
import { yupObjectSchema } from "components/utils/yupUtils";
import * as yup from "yup";

export interface RestorePoint {
    dateTime: string;
    location: string;
    fileName: string;
    isSnapshotRestore: boolean;
    isIncremental: boolean;
    isEncrypted: boolean;
    filesToRestore: number;
    databaseName: string;
    nodeTag: string;
    backupType: string;
}

const basicInfoSchema = yup.object({
    databaseName: yup
        .string()
        .required()
        .when("$usedDatabaseNames", ([usedDatabaseNames], schema) =>
            schema.notOneOf(usedDatabaseNames, "Database already exists")
        ),
    isSharded: yup.boolean(),
});

const restorePointsSchema = yup.array().of(
    yup.object({
        nodeTag: yup.string().required(),
        restorePoint: yupObjectSchema<RestorePoint | null>({
            dateTime: yup.string().required(),
            location: yup.string().required(),
            fileName: yup.string().required(),
            isSnapshotRestore: yup.boolean().required(),
            isIncremental: yup.boolean().required(),
            isEncrypted: yup.boolean().required(),
            filesToRestore: yup.number().required(),
            databaseName: yup.string().required(),
            nodeTag: yup.string().required(),
            backupType: yup.string().required(),
        }),
    })
);

const localSource = yup.object({
    directory: yup.string().when("$sourceType", {
        is: "local",
        then: (schema) => schema.required(),
    }),
    restorePoints: restorePointsSchema.min(1),
});

const ravenCloudSource = yup.object({
    link: yup.string().when("$sourceType", {
        is: "ravenCloud",
        then: (schema) => schema.required(),
    }),
    restorePoints: restorePointsSchema.min(1),
});

const amazonS3Source = yup.object({
    isUseCustomHost: yup.boolean(),
    isForcePathStyle: yup.boolean(),
    customHost: yup.string().when(["isUseCustomHost", "$sourceType"], {
        is: (isUseCustomHost: boolean, sourceType: restoreSource) => isUseCustomHost && sourceType === "amazonS3",
        then: (schema) => schema.required(),
    }),
    accessKey: yup.string().when("$sourceType", {
        is: "aws",
        then: (schema) => schema.required(),
    }),
    secretKey: yup.string().when("$sourceType", {
        is: "aws",
        then: (schema) => schema.required(),
    }),
    awsRegion: yup.string().when("$sourceType", {
        is: "aws",
        then: (schema) => schema.required(),
    }),
    bucketName: yup.string().when("$sourceType", {
        is: "aws",
        then: (schema) => schema.required(),
    }),
    remoteFolderName: yup.string(),
    restorePoints: restorePointsSchema.min(1),
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
    restorePoints: restorePointsSchema.min(1),
});

const googleCloudSource = yup.object({
    bucketName: yup.string().when("$sourceType", {
        is: "gcp",
        then: (schema) => schema.required(),
    }),
    credentialsJson: yup.string().when("$sourceType", {
        is: "gcp",
        then: (schema) => schema.required(),
    }),
    remoteFolderName: yup.string(),
    restorePoints: restorePointsSchema.min(1),
});

// type SourceData = yup.InferType<
//     typeof localSource | typeof ravenCloudSource | typeof amazonS3Source | typeof azureSource | typeof googleCloudSource
// >;

const sourceSchema = yup.object({
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
    basicInfo: basicInfoSchema,
    source: sourceSchema,
    encryption: encryptionSchema,
    pathsConfigurations: pathsConfigurationsSchema,
});

export type CreateDatabaseFromBackupFormData = yup.InferType<typeof createDatabaseFromBackupSchema>;
