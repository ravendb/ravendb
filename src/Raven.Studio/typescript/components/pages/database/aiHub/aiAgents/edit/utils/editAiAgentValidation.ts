import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    name: yup.string().required(),
    connectionStringName: yup.string().required(),
    systemPrompt: yup.string().required(),
    sampleObject: yup.string(),
    outputSchema: yup
        .string()
        .test(
            "sampleObjectOrJsonSchema",
            "Either 'Sample response object' or 'JSON schema' must be provided",
            function (_, { parent }) {
                return !!parent.sampleObject || !!parent.outputSchema;
            }
        ),
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
            parametersSampleObject: yup.string(),
            parametersSchema: yup
                .string()
                .test(
                    "sampleObjectOrJsonSchema",
                    "Either 'Sample response object' or 'JSON schema' must be provided",
                    function (_, { parent }) {
                        return !!parent.parametersSampleObject || !!parent.parametersSchema;
                    }
                ),
            isSaved: yup.boolean(),
            isEditing: yup.boolean(),
        })
    ),
    actions: yup.array().of(
        yup.object({
            name: yup.string().required(),
            description: yup.string().required(),
            parametersSampleObject: yup.string(),
            parametersSchema: yup
                .string()
                .test(
                    "sampleObjectOrJsonSchema",
                    "Either 'Sample response object' or 'JSON schema' must be provided",
                    function (_, { parent }) {
                        return !!parent.parametersSampleObject || !!parent.parametersSchema;
                    }
                ),
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
