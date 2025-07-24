import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

export type AiAgentTrimmingMethod = "Tokens" | "Truncate";

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
    isDocumentExpireInCustomizeEnabled: yup.boolean(),
    persistenceConversationIdPrefix: yup.string().required(),
    persistenceExpiresInSeconds: yup.number().nullable().positive().integer(),
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
    isToolsAdvancedSettings: yup.boolean(),
    queries: yup.array().of(
        yup.object({
            name: yup
                .string()
                .required()
                .matches(/^[a-zA-Z0-9_-]+$/, "Tool name can only contain letters, numbers, underscores and hyphens"),
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
            name: yup
                .string()
                .required()
                .matches(/^[a-zA-Z0-9_-]+$/, "Tool name can only contain letters, numbers, underscores and hyphens"),
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
    maxModelIterationsPerCall: yup.number().nullable().positive().integer(),

    trimming: yup
        .object({
            method: yup.string<"Tokens" | "Truncate">().nullable(),
            isEnableHistory: yup.boolean(),
            isSetHistoryExpiration: yup.boolean(),
            historyExpirationInSeconds: yup.number().nullable().positive().integer(),
            messagesLengthBeforeTruncate: yup.number().nullable().positive().integer(),
            messagesLengthAfterTruncate: yup.number().nullable().positive().integer(),
            maxTokensBeforeSummarization: yup.number().nullable().positive().integer(),
            maxTokensAfterSummarization: yup.number().nullable().positive().integer(),
            resultPrefix: yup.string().nullable(),
            summarizationTaskBeginningPrompt: yup.string().nullable(),
            summarizationTaskEndPrompt: yup.string().nullable(),
        })
        .nullable(),

    test: yup.object({
        prompt: yup.string().nullable(),
        parameters: yup.array().of(
            yup.object({
                name: yup.string().nullable(),
                value: yup.string().nullable(),
            })
        ),
    }),
});

export const editAiAgentYupResolver = yupResolver(schema);
export type EditAiAgentFormData = yup.InferType<typeof schema>;
