import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDocumentExpirationEnabled: yup.boolean(),
        isDeleteFrequencyEnabled: yup.boolean(),
        deleteFrequency: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("isDeleteFrequencyEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
        isLimitMaxItemsToProcessEnabled: yup.boolean(),
        maxItemsToProcess: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("isLimitMaxItemsToProcessEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
    })
    .required();

export const documentExpirationYupResolver = yupResolver(schema);
export type DocumentExpirationFormData = yup.InferType<typeof schema>;
