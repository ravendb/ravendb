import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup.object({
    prefix: yup.string().required(),
    value: yup.number().required(),
});

export const addIdentitiesYupResolver = yupResolver(schema);
export type AddIdentitiesFormData = yup.InferType<typeof schema>;
