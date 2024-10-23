import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDocumentRefreshEnabled: yup.boolean(),
        isRefreshFrequencyEnabled: yup.boolean(),
        refreshFrequency: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("isRefreshFrequencyEnabled", {
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

export const documentRefreshYupResolver = yupResolver(schema);
export type DocumentRefreshFormData = yup.InferType<typeof schema>;
