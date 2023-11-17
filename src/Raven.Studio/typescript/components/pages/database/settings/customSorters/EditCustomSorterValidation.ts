import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        Name: yup.string(),
        Code: yup.string(),
    })
    .required();

export const editCustomSorterScriptYupResolver = yupResolver(schema);
export type EditCustomSorterScriptFormData = yup.InferType<typeof schema>;
