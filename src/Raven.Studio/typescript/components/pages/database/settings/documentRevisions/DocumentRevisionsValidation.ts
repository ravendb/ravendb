import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const editCollectionConfigSchema = yup.object({
    CollectionName: yup.string().nullable().required(),
    Disabled: yup.boolean(),
    IsPurgeOnDeleteEnabled: yup.boolean(),
    IsMinimumRevisionsToKeepEnabled: yup.boolean(),
    MinimumRevisionsToKeep: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("IsMinimumRevisionsToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    IsMinimumRevisionAgeToKeepEnabled: yup.boolean(),
    MinimumRevisionAgeToKeep: yup
        .string()
        .nullable()
        .when("IsMinimumRevisionAgeToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled: yup.boolean(),
    MaximumRevisionsToDeleteUponDocumentUpdate: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const editConfigSchema = editCollectionConfigSchema.omit(["CollectionName"]);

export const documentRevisionsConfigYupResolver = yupResolver(editConfigSchema);
export const documentRevisionsCollectionConfigYupResolver = yupResolver(editCollectionConfigSchema);

export type EditDocumentRevisionsConfig = Required<yup.InferType<typeof editConfigSchema>>;
export type EditDocumentRevisionsCollectionConfig = Required<yup.InferType<typeof editCollectionConfigSchema>>;
