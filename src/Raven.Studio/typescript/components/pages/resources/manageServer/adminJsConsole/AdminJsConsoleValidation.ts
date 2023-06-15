import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        target: yup.string().required(),
        scriptText: yup.string().required(),
    })
    .required();

export const adminJsConsoleYupResolver = yupResolver(schema);
export type AdminJsConsoleFormData = yup.InferType<typeof schema>;
