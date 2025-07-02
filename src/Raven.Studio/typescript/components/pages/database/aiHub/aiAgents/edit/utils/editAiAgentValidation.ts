import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    name: yup.string().required(),
    connectionStringName: yup.string().required(),
    systemPrompt: yup.string().required(),
    outputSchema: yup.string().required(),
    persistenceCollectionName: yup.string().required(),
    persistenceExpires: yup.string().required(),
    parameters: yup.object(),
    queries: yup.array().of(
        yup.object({
            name: yup.string().required(),
            description: yup.string().required(),
            query: yup.string().required(),
            parametersSchema: yup.array().of(
                yup.object({
                    parameter: yup.string().required(),
                    description: yup.string().required(),
                })
            ),
            isSaved: yup.boolean(),
            isEditing: yup.boolean(),
        })
    ),

    // test
    prompt: yup.string(),
});

export const editAiAgentYupResolver = yupResolver(schema);
export type EditAiAgentFormData = yup.InferType<typeof schema>;
