import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        isDocumentRefreshEnabled: yup.boolean(),
        isRefreshFrequencyEnabled: yup.boolean(),
        isRefreshFrequencyInSec: yup.number(),
    })
    .required();

export const documentRefreshYupResolver = yupResolver(schema);
export type DocumentRefreshFormData = yup.InferType<typeof schema>;
