import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { FieldPath } from "react-hook-form";
import {
    documentsSchema,
    configurationSchema,
    transformScriptSchema,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";

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
            then: (schema) => schema.min(1, "Value must be at least 1").required("Value is required"),
        }),
    isEncrypted: yup.boolean(),
    encryptionKey: yup.string().when("isEncrypted", {
        is: true,
        then: (schema) => schema.required("Encryption key is required"),
    }),
});

export const importFromFileSchema = yup.object({
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

export type ImportFromFileFormPath = FieldPath<ImportFromFileFormData>;

export const importFromFileYupResolver = yupResolver(importFromFileSchema);
