import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const minimumEntriesAgeToKeepMessage =
    "The age must be greater than 0. To delete all items in the Revisions Bin, toggle 'Set minimum entries age to keep' off.";

const schema = yup.object({
    isRevisionsBinCleanerEnabled: yup.boolean(),
    isMinimumEntriesAgeToKeepEnabled: yup.boolean(),
    minimumEntriesAgeToKeep: yup
        .number()
        .nullable()
        .positive(minimumEntriesAgeToKeepMessage)
        .integer()
        .when("isMinimumEntriesAgeToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(minimumEntriesAgeToKeepMessage),
        }),
    isCleanerFrequencyInSecEnabled: yup.boolean(),
    cleanerFrequencyInSec: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isCleanerFrequencyInSecEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

export const revisionsBinCleanerYupResolver = yupResolver(schema);
export type RevisionsBinCleanerFormData = yup.InferType<typeof schema>;
