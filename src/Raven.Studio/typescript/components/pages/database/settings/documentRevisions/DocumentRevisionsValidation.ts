import * as yup from "yup";
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import { yupResolver } from "@hookform/resolvers/yup";
import { yupObjectSchema } from "components/utils/yupUtils";

const schema = yupObjectSchema<RevisionsCollectionConfiguration>({
    CollectionSpecificName: yup.string(),
    Disabled: yup.boolean(),
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
    IsMinimumRevisionAgeToKeepEnabled: yup.boolean(),
    MinimumRevisionAgeToKeep: yup.string().when("IsMinimumRevisionAgeToKeepEnabled", {
        is: true,
        then: (schema) => schema.required(),
    }),
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
    PurgeOnDelete: yup.boolean(),
});

export const documentRevisionsYupResolver = yupResolver(schema);
export type DocumentRevisionsFormData = Required<yup.InferType<typeof schema>>;
