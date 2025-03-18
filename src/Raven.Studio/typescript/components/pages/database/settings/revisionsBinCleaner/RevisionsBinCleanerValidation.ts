import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup.object({
    isRevisionsBinCleanerEnabled: yup.boolean(),
    isMinimumEntriesAgeToKeepEnabled: yup.boolean(),
    minimumEntriesAgeToKeep: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isMinimumEntriesAgeToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(),
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
