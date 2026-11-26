import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDocumentExpirationEnabled: yup.boolean(),
        deleteFrequency: yup.number().nullable().positive().integer(),
        maxItemsToProcess: yup.number().nullable().positive().integer(),
    })
    .required();

export const documentExpirationYupResolver = yupResolver(schema);
export type DocumentExpirationFormData = yup.InferType<typeof schema>;
