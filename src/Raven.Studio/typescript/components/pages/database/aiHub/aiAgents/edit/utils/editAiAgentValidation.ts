import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const schema = yup.object({
    name: yup.string().required(),
    identifier: yup.string().required(),
    connectionStringName: yup.string().required(),
    systemPrompt: yup.string().required(),
    sampleObject: yup.string().nullable(),
    outputSchema: yup
        .string()
        .nullable()
        .test(
            "sampleObjectOrJsonSchema",
            "Either 'Sample response object' or 'JSON schema' must be provided",
            function (_, { parent }) {
                return !!parent.sampleObject || !!parent.outputSchema;
            }
        ),
    isEnableDocumentExpiration: yup.boolean(),
    persistenceConversationIdPrefix: yup.string().required(),
    persistenceExpiresInSeconds: yup.number().nullable(),
    parameterInput: yup.string().test("unique-parameter", "Parameter name must be unique", function (value) {
        if (!value) {
            return true;
        }
        const parameters = this.parent.parameters || [];
        return !parameters.some((param: { name: string }) => param.name === value);
    }),
    parameters: yup.array().of(
        yup.object({
            name: yup.string(),
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
            name: yup.string().nullable(),
            value: yup.string().nullable(),
        })
    ),
});

export const editAiAgentYupResolver = yupResolver(schema);
export type EditAiAgentFormData = yup.InferType<typeof schema>;
