import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDataArchivalEnabled: yup.boolean(),
        isArchiveFrequencyEnabled: yup.boolean(),
        archiveFrequency: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("isArchiveFrequencyEnabled", { is: true, then: (schema) => schema.required() }),
    })
    .required();

export const dataArchivalYupResolver = yupResolver(schema);
export type DataArchivalFormData = yup.InferType<typeof schema>;
