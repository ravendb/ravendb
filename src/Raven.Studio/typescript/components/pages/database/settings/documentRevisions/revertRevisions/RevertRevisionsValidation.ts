import { yupResolver } from "@hookform/resolvers/yup";
import genUtils from "common/generalUtils";
import moment from "moment";
import * as yup from "yup";

const schema = yup.object({
    pointInTime: yup.date().nullable().required().max(moment().add(10, "minutes").format(genUtils.inputDateTimeFormat)),
    timeWindow: yup.number().nullable().positive().integer(),
    timeMagnitude: yup.mixed<timeMagnitude>().oneOf(["minutes", "hours", "days"]),
    isRevertAllCollections: yup.boolean(),
    collections: yup.array().of(yup.string()),
});

export const revertRevisionsYupResolver = yupResolver(schema);
export type RevertRevisionsFormData = Required<yup.InferType<typeof schema>>;
