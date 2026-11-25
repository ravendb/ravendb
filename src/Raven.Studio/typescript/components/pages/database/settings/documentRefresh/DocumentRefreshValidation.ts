import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDocumentRefreshEnabled: yup.boolean(),
        refreshFrequency: yup.number().nullable().positive().integer(),
        maxItemsToProcess: yup.number().nullable().positive().integer(),
    })
    .required();

export const documentRefreshYupResolver = yupResolver(schema);
export type DocumentRefreshFormData = yup.InferType<typeof schema>;
