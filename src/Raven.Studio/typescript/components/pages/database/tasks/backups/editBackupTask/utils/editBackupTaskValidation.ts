import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { destinationsSchema } from "components/common/formDestinations/utils/formDestinationsValidation";
import { cronEditorUtils } from "components/common/cronEditor/cronEditorUtils";
import { createTranslator } from "common/i18n/i18n";
import { TimeInSeconds } from "common/constants/timeInSeconds";

const t = createTranslator("editBackupTask");

type BackupType = Raven.Client.Documents.Operations.Backups.BackupType;
type BackupUploadMode = Raven.Client.Documents.Operations.Backups.BackupUploadMode;
type EncryptionMode = Raven.Client.Documents.Operations.Backups.EncryptionMode;
type CompressionLevel = System.IO.Compression.CompressionLevel;
type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export type SnapshotCompressionAlgorithm = Sparrow.Backups.SnapshotBackupCompressionAlgorithm | "Default";

export const MIN_RETENTION_IN_SEC = TimeInSeconds.Day;

export type CronServerErrors = Map<string, string>;

export interface EditBackupTaskValidationContext {
    isManual: boolean;
    cronServerErrors: CronServerErrors;
    isDatabaseEncrypted: boolean;
}

function getCronError(expression: string, serverErrors: CronServerErrors | undefined): string | null {
    return cronEditorUtils.getValidationError(expression) ?? serverErrors?.get(expression) ?? null;
}

function cronSchema(isEnabledField: string) {
    return yup
        .string()
        .nullable()
        .when(isEnabledField, {
            is: true,
            then: (schema) =>
                schema
                    .required(() => t("validation.selectSchedule"))
                    .test("cron", (value, ctx) => {
                        const error = getCronError(value, ctx.options.context?.cronServerErrors);
                        return error ? ctx.createError({ message: error }) : true;
                    }),
        });
}

const editBackupTaskSchema = yup
    .object({
        backupType: yup
            .string<BackupType>()
            .nullable()
            .test(
                "required",
                () => t("validation.selectBackupType"),
                (value) => value != null
            ),
        name: yup.string().nullable(),
        state: yup.string<OngoingTaskState>(),
        backupUploadMode: yup.string<BackupUploadMode>(),
        snapshotCompressionAlgorithm: yup.string<SnapshotCompressionAlgorithm>(),
        snapshotCompressionLevel: yup.string<CompressionLevel>(),
        isSnapshotExcludeIndexes: yup.boolean(),

        isSetResponsibleNode: yup.boolean(),
        responsibleNode: yup
            .string()
            .nullable()
            .when("isSetResponsibleNode", {
                is: true,
                then: (schema) => schema.required(() => t("validation.selectNode")),
            }),
        isPinResponsibleNode: yup.boolean(),

        isSetMaxReadOpsPerSecond: yup.boolean(),
        maxReadOpsPerSecond: yup
            .number()
            .nullable()
            .when("isSetMaxReadOpsPerSecond", {
                is: true,
                then: (schema) => schema.required().integer().positive(),
            }),

        isSetRetentionPolicy: yup.boolean(),
        minimumBackupAgeToKeepInSec: yup
            .number()
            .nullable()
            .when("isSetRetentionPolicy", {
                is: true,
                then: (schema) => schema.required().min(MIN_RETENTION_IN_SEC, () => t("validation.minRetention")),
            }),

        isEncrypted: yup.boolean(),
        isUnencryptedBackupAllowed: yup.boolean().test(
            "unencrypted-backup-consent",
            () => t("validation.confirmUnencrypted"),
            (value, ctx) => !ctx.options.context?.isDatabaseEncrypted || ctx.parent.isEncrypted || value
        ),
        encryptionMode: yup.string<EncryptionMode>(),
        encryptionKey: yup
            .string()
            .nullable()
            .when(["isEncrypted", "encryptionMode"], {
                is: (isEncrypted: boolean, encryptionMode: EncryptionMode) =>
                    isEncrypted && encryptionMode === "UseProvidedKey",
                then: (schema) => schema.required().base64(),
            }),
        originalEncryptionKey: yup.string().nullable(),
        isEncryptionKeySaved: yup
            .boolean()
            .when(["isEncrypted", "encryptionMode", "encryptionKey", "originalEncryptionKey"], {
                is: (isEncrypted: boolean, encryptionMode: EncryptionMode, key: string, originalKey: string) =>
                    isEncrypted && encryptionMode === "UseProvidedKey" && key !== originalKey,
                then: (schema) => schema.oneOf([true], () => t("validation.confirmKeySaved")),
            }),

        isFullBackupEnabled: yup.boolean(),
        fullBackupFrequency: cronSchema("isFullBackupEnabled"),
        isIncrementalBackupEnabled: yup.boolean().test(
            "at-least-one-schedule",
            () => t("validation.enableSchedule"),
            (value, ctx) => ctx.options.context?.isManual || value || ctx.parent.isFullBackupEnabled
        ),
        incrementalBackupFrequency: cronSchema("isIncrementalBackupEnabled"),
    })
    .concat(destinationsSchema);

export type EditBackupTaskFormData = yup.InferType<typeof editBackupTaskSchema>;

export const editBackupTaskYupResolver = yupResolver(editBackupTaskSchema);

export const editBackupTaskFieldsByStep = {
    type: ["backupType"],
    settings: [
        "name",
        "state",
        "backupUploadMode",
        "snapshotCompressionAlgorithm",
        "snapshotCompressionLevel",
        "isSnapshotExcludeIndexes",
        "responsibleNode",
        "maxReadOpsPerSecond",
        "minimumBackupAgeToKeepInSec",
        "isEncrypted",
        "isUnencryptedBackupAllowed",
        "encryptionKey",
        "isEncryptionKeySaved",
    ],
    schedule: ["isIncrementalBackupEnabled", "fullBackupFrequency", "incrementalBackupFrequency"],
    destinations: ["destinations"],
} satisfies Record<string, (keyof EditBackupTaskFormData)[]>;
