import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    prompt: yup.string().required(),
    parameters: yup.array().of(
        yup.object({
            name: yup.string().required(),
            value: yup.string().required(),
        })
    ),
});

export const chatAiAgentYupResolver = yupResolver(schema);
export type ChatAiAgentFormData = yup.InferType<typeof schema>;
