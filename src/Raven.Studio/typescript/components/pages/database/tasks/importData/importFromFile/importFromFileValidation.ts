import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { FieldPath } from "react-hook-form";
import {
    documentsSchema,
    configurationSchema,
    transformScriptSchema,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("importData");

const collectionsSchema = yup.object({
    isImportAllCollections: yup.boolean(),
    includedCollections: yup.array().of(yup.string()),
});

const processingSchema = transformScriptSchema.shape({
    isSetMaxReadOpsPerSecond: yup.boolean(),
    maxReadOpsPerSecond: yup
        .number()
        .nullable()
        .when("isSetMaxReadOpsPerSecond", {
            is: true,
            then: (schema) =>
                schema
                    .min(1, () => t("importFile.validation.minValue"))
                    .required(() => t("importFile.validation.valueRequired")),
        }),
    isEncrypted: yup.boolean(),
    encryptionKey: yup.string().when("isEncrypted", {
        is: true,
        then: (schema) => schema.required(() => t("importFile.validation.encryptionKeyRequired")),
    }),
});

export const importFromFileSchema = yup.object({
    file: yup
        .mixed<File>()
        .required(() => t("importFile.validation.fileRequired"))
        .test(
            "not-snapshot",
            () => t("importFile.validation.snapshotFile"),
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

export type ImportFromFileFormPath = FieldPath<ImportFromFileFormData>;

export const importFromFileYupResolver = yupResolver(importFromFileSchema);
