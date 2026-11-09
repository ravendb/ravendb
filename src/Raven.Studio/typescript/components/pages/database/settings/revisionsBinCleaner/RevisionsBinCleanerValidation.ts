import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const minimumEntriesAgeToKeepMessage =
    "The age must be greater than 0. To delete all items in the Revisions Bin, toggle 'Set minimum entries age to keep' off.";

const schema = yup.object({
    isRevisionsBinCleanerEnabled: yup.boolean(),
    isMinimumEntriesAgeToKeepEnabled: yup.boolean(),
    minimumEntriesAgeToKeep: yup.number().nullable().positive(minimumEntriesAgeToKeepMessage).integer(),
    cleanerFrequencyInSec: yup.number().nullable().positive().integer(),
});

export const revisionsBinCleanerYupResolver = yupResolver(schema);
export type RevisionsBinCleanerFormData = yup.InferType<typeof schema>;
