import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    pointInTime: yup.date().nullable().required(),
    timeWindow: yup.number().nullable().positive().integer(),
    timeMagnitude: yup.mixed<timeMagnitude>().oneOf(["minutes", "hours", "days"]),
    isRevertAllCollections: yup.boolean(),
    collections: yup.array().of(yup.string()),
});

export const revertRevisionsYupResolver = yupResolver(schema);
export type RevertRevisionsFormData = yup.InferType<typeof schema>;
