import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

export type AiAgentTrimmingMethod = "Tokens" | "Truncate";

const schema = yup.object({
    // Basic
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
        )
        .test(
            "schemaRegenerationRequired",
            "The sample object has been modified. Please regenerate the JSON schema to ensure it matches the new sample object structure",
            function (_, { parent }) {
                return !parent.canRegenerateSchema;
            }
        ),
    canRegenerateSchema: yup.boolean(),

    // Persistence
    isEnableDocumentExpiration: yup.boolean(),
    isDocumentExpireInCustomizeEnabled: yup.boolean(),
    persistenceConversationIdPrefix: yup.string().required(),
    persistenceExpiresInSeconds: yup.number().nullable().positive().integer(),

    // Parameters
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

    // Tools
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
                )
                .test(
                    "schemaRegenerationRequired",
                    "The sample object has been modified. Please regenerate the JSON schema to ensure it matches the new sample object structure",
                    function (_, { parent }) {
                        return !parent.canRegenerateSchema;
                    }
                ),
            canRegenerateSchema: yup.boolean(),
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
                )
                .test(
                    "schemaRegenerationRequired",
                    "The sample object has been modified. Please regenerate the JSON schema to ensure it matches the new sample object structure",
                    function (_, { parent }) {
                        return !parent.canRegenerateSchema;
                    }
                ),
            canRegenerateSchema: yup.boolean(),
            isSaved: yup.boolean(),
            isEditing: yup.boolean(),
        })
    ),

    // Trimming
    trimming: yup
        .object({
            method: yup.string<"Tokens" | "Truncate">().nullable(),

            // History
            isEnableHistory: yup.boolean(),
            isSetHistoryExpiration: yup.boolean(),
            historyExpirationInSeconds: yup.number().nullable().positive().integer(),

            // Truncate
            messagesLengthBeforeTruncate: yup.number().nullable().positive().integer(),
            messagesLengthAfterTruncate: yup.number().nullable().positive().integer(),

            // Tokens
            maxTokensBeforeSummarization: yup.number().nullable().positive().integer(),
            maxTokensAfterSummarization: yup.number().nullable().positive().integer(),
        })
        .nullable(),

    // Test
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
