import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    name: yup.string().required(),
    connectionStringName: yup.string().required(),
    systemPrompt: yup.string().required(),
    outputSchema: yup.string().required(),
    persistenceCollectionName: yup.string().required(),
    persistenceExpiresInSeconds: yup.number(),
    parameterInput: yup.string().test("unique-parameter", "Parameter name must be unique", function (value) {
        if (!value) {
            return true;
        }
        const parameters = this.parent.parameters || [];
        return !parameters.some((param: { name: string }) => param.name === value);
    }),
    parameters: yup.array().of(
        yup.object({
            name: yup.string().required(),
        })
    ),
    queries: yup.array().of(
        yup.object({
            name: yup.string().required(),
            description: yup.string().required(),
            query: yup.string().required(),
            parametersSchema: yup.string(),
            isSaved: yup.boolean(),
            isEditing: yup.boolean(),
        })
    ),
    actions: yup.array().of(
        yup.object({
            name: yup.string().required(),
            description: yup.string().required(),
            parametersSchema: yup.string(),
            isSaved: yup.boolean(),
            isEditing: yup.boolean(),
        })
    ),

    // test
    testPrompt: yup.string(),
    testParameters: yup.array().of(
        yup.object({
            name: yup.string().required(),
            value: yup.string().required(),
        })
    ),
});

export const editAiAgentYupResolver = yupResolver(schema);
export type EditAiAgentFormData = yup.InferType<typeof schema>;
