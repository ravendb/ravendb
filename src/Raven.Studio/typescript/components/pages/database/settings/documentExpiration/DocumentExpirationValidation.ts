import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDocumentExpirationEnabled: yup.boolean(),
        isExpirationFrequencyEnabled: yup.boolean(),
        expirationFrequency: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("isExpirationFrequencyEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
    })
    .required();

export const documentExpirationYupResolver = yupResolver(schema);
export type DocumentExpirationFormData = yup.InferType<typeof schema>;
