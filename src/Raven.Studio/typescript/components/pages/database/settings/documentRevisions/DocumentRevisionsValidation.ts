import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const editCollectionConfigSchema = yup.object({
    collectionName: yup.string().nullable().required(),
    disabled: yup.boolean(),
    isPurgeOnDeleteEnabled: yup.boolean(),
    isMinimumRevisionsToKeepEnabled: yup.boolean(),
    minimumRevisionsToKeep: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("IsMinimumRevisionsToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isMinimumRevisionAgeToKeepEnabled: yup.boolean(),
    minimumRevisionAgeToKeep: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("IsMinimumRevisionAgeToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isMaximumRevisionsToDeleteUponDocumentUpdateEnabled: yup.boolean(),
    maximumRevisionsToDeleteUponDocumentUpdate: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const editConfigSchema = editCollectionConfigSchema.omit(["collectionName"]);

export const documentRevisionsConfigYupResolver = yupResolver(editConfigSchema);
export const documentRevisionsCollectionConfigYupResolver = yupResolver(editCollectionConfigSchema);

export type EditDocumentRevisionsCollectionConfig = Required<yup.InferType<typeof editCollectionConfigSchema>>;
