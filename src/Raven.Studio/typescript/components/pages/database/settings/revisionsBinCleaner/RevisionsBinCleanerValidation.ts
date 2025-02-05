import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup.object({
    isRevisionsBinCleanerEnabled: yup.boolean(),
    isMinimumEntriesAgeToKeepEnabled: yup.boolean(),
    minimumEntriesAgeToKeepInMin: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isMinimumEntriesAgeToKeepEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isRefreshFrequencyEnabled: yup.boolean(),
    timeMagnitude: yup.mixed<timeMagnitude>().oneOf(["minutes", "hours", "days"]),
    refreshFrequencyInSec: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isRefreshFrequencyEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

export const revisionsBinCleanerYupResolver = yupResolver(schema);
export type RevisionsBinCleanerFormData = yup.InferType<typeof schema>;
