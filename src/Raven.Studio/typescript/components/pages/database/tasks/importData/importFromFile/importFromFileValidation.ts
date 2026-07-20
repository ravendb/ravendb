import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

export const databaseSettingKeys = [
    "settings",
    "conflictSolverConfig",
    "client",
    "revisions",
    "refresh",
    "expiration",
    "documentsCompression",
    "schemaValidation",
    "dataArchival",
    "timeSeries",
    "sorters",
    "analyzers",
    "postgreSqlIntegration",
] as const;

export type DatabaseSettingKey = (typeof databaseSettingKeys)[number];

export const ongoingTaskKeys = [
    "periodicBackups",
    "externalReplications",
    "ravenEtls",
    "sqlEtls",
    "snowflakeEtls",
    "olapEtls",
    "elasticSearchEtls",
    "queueEtls",
    "hubReplications",
    "sinkReplications",
    "embeddingsGeneration",
    "genAi",
    "cdcSinks",
    "aiAgents",
    "remoteAttachments",
] as const;

export type OngoingTaskKey = (typeof ongoingTaskKeys)[number];

export const connectionStringKeys = [
    "ravenConnectionStrings",
    "sqlConnectionStrings",
    "snowflakeConnectionStrings",
    "olapConnectionStrings",
    "elasticSearchConnectionStrings",
    "queueConnectionStrings",
    "aiConnectionStrings",
] as const;

export type ConnectionStringKey = (typeof connectionStringKeys)[number];

const documentsSchema = yup.object({
    isIncludeDocuments: yup.boolean(),
    isIncludeAttachments: yup.boolean(),
    isIncludeCounters: yup.boolean(),
    isIncludeRevisions: yup.boolean(),
    isIncludeTimeSeries: yup.boolean(),
    isIncludeTimeSeriesDeletedRanges: yup.boolean(),
    isIncludeArtificialDocuments: yup.boolean(),
    isIncludeArchivedDocuments: yup.boolean(),
    isIncludeExpiredDocuments: yup.boolean(),
    isIncludeConflicts: yup.boolean(),
    isIncludeCompareExchange: yup.boolean(),
    isIncludeLegacyAttachments: yup.boolean(),
    isIncludeDocumentsTombstones: yup.boolean(),
    isIncludeCompareExchangeTombstones: yup.boolean(),
    isIncludeSubscriptions: yup.boolean(),
});

const collectionsSchema = yup.object({
    isImportAllCollections: yup.boolean(),
    includedCollections: yup.array().of(yup.string()),
});

const configurationSchema = yup.object({
    isIncludeIndexes: yup.boolean(),
    isIncludeIndexHistory: yup.boolean(),
    isRemoveAnalyzers: yup.boolean(),
    isIncludeIdentities: yup.boolean(),
    isIncludeConnectionStringsAndOngoingTasks: yup.boolean(),
    isCustomizeOngoingTasks: yup.boolean(),
    ongoingTasks: yup.object(Object.fromEntries(ongoingTaskKeys.map((key) => [key, yup.boolean()]))),
    connectionStrings: yup.object(Object.fromEntries(connectionStringKeys.map((key) => [key, yup.boolean()]))),
    isImportAllSettings: yup.boolean(),
    databaseSettings: yup.object(Object.fromEntries(databaseSettingKeys.map((key) => [key, yup.boolean()]))),
});

const processingSchema = yup.object({
    isUseTransformScript: yup.boolean(),
    transformScript: yup.string().when("isUseTransformScript", {
        is: true,
        then: (schema) => schema.required("Transform script is required when enabled"),
    }),
    isSetMaxReadOpsPerSecond: yup.boolean(),
    maxReadOpsPerSecond: yup
        .number()
        .nullable()
        .when("isSetMaxReadOpsPerSecond", {
            is: true,
            then: (schema) => schema.min(1, "Value must be at least 1").required("Value is required"),
        }),
    isEncrypted: yup.boolean(),
    encryptionKey: yup.string().when("isEncrypted", {
        is: true,
        then: (schema) => schema.required("Encryption key is required"),
    }),
});

export const importFromFileSchema = yup.object({
    // File object kept outside yup type-checking; validated for presence + non-snapshot extension
    file: yup
        .mixed<File>()
        .required("Select a file to import")
        .test(
            "not-snapshot",
            "The selected file is a RavenDB Snapshot file and cannot be imported. " +
                "Use the 'Restore' option (under Create New Database) in order to restore data from a RavenDB Snapshot file.",
            (file) => {
                if (!file) {
                    return true;
                }
                const extension = file.name.split(".").pop()?.toLowerCase();
                return !["ravendb-snapshot", "ravendb-encrypted-snapshot"].includes(extension);
            }
        ),
    documents: documentsSchema,
    collections: collectionsSchema,
    configuration: configurationSchema,
    processing: processingSchema,
});

export type ImportFromFileFormData = yup.InferType<typeof importFromFileSchema>;

export const importFromFileYupResolver = yupResolver(importFromFileSchema);
