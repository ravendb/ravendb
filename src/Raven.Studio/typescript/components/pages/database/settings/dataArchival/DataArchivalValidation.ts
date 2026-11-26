import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDataArchivalEnabled: yup.boolean(),
        archiveFrequency: yup.number().nullable().positive().integer(),
        maxItemsToProcess: yup.number().nullable().positive().integer(),
    })
    .required();

export const dataArchivalYupResolver = yupResolver(schema);
export type DataArchivalFormData = yup.InferType<typeof schema>;
