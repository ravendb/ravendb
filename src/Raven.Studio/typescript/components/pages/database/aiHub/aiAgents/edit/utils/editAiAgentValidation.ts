import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    name: yup.string().required(),
    connectionStringName: yup.string().required(),
    systemPrompt: yup.string().required(),
    outputSchema: yup.string().required(),
    persistenceCollectionName: yup.string().required(),
    persistenceExpires: yup.string().required(),
    parameters: yup.object().required(),
    queries: yup.array().of(
        yup.object({
            name: yup.string().required(),
            description: yup.string().required(),
            query: yup.string().required(),
            parametersSchema: yup.string().required(),
        })
    ),

    // test
    prompt: yup.string().required(),
});

export const editAiAgentYupResolver = yupResolver(schema);
export type EditAiAgentFormData = yup.InferType<typeof schema>;
