import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { aiAgentParametersUtils } from "../../utils/aiAgentParametersUtils";

type AiAgentParameterValueType = Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType;

export type AiAgentTrimmingMethod = "Tokens";

export interface EditAiAgentValidationContext {
    allParameterNames: string[];
    allQueryNames: string[];
    allActionNames: string[];
}

const editSchema = yup.object({
    // Basic
    name: yup.string().required(),
    identifier: yup.string().required(),
    state: yup.string<"Disabled" | "Enabled">().required(),
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

    // Parameters
    parameters: yup.array().of(
        yup.object({
            name: yup
                .string()
                .nullable()
                .required()
                .test(
                    "unique-parameter",
                    "Parameter name must be unique",
                    function (value, ctx: yup.TestContext<EditAiAgentValidationContext>) {
                        const allParameterNames = ctx.options.context.allParameterNames ?? [];
                        const valuesCount = allParameterNames.filter((name: string) => name === value).length;
                        return valuesCount <= 1;
                    }
                ),
            description: yup.string().nullable(),
            isSendToModel: yup.boolean(),
            policy: yup.string<Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterPolicy>(),
            type: yup.string<Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType>(),
            isEditing: yup.boolean(),
        })
    ),

    // Tools
    queries: yup.array().of(
        yup.object({
            name: yup
                .string()
                .required()
                .matches(/^[a-zA-Z0-9_-]+$/, "Tool name can only contain letters, numbers, underscores and hyphens")
                .test("unique-name", "Tool name must be unique", getIsToolNameUnique),
            description: yup.string().required(),
            isAllowModelQueries: yup
                .boolean()
                .nullable()
                .when("isAllowModelQueriesOverride", {
                    is: true,
                    then: (schema) => schema.required(),
                }),
            isAllowModelQueriesOverride: yup.boolean(),
            isAddToInitialContext: yup
                .boolean()
                .nullable()
                .when("isAddToInitialContextOverride", {
                    is: true,
                    then: (schema) => schema.required(),
                }),
            isAddToInitialContextOverride: yup.boolean(),
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
            isEditing: yup.boolean(),
        })
    ),
    actions: yup.array().of(
        yup.object({
            name: yup
                .string()
                .required()
                .matches(/^[a-zA-Z0-9_-]+$/, "Tool name can only contain letters, numbers, underscores and hyphens")
                .test("unique-name", "Tool name must be unique", getIsToolNameUnique),
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
            isEditing: yup.boolean(),
        })
    ),

    // Trimming
    trimming: yup
        .object({
            method: yup.string<AiAgentTrimmingMethod>().nullable(),

            // History
            isEnableHistory: yup.boolean(),
            isSetHistoryExpiration: yup.boolean(),
            historyExpirationInSeconds: yup.number().nullable().positive().integer(),

            // Truncate
            // messagesLengthBeforeTruncate: yup.number().nullable().positive().integer(),
            // messagesLengthAfterTruncate: yup.number().nullable().positive().integer(),

            // Tokens
            maxTokensBeforeSummarization: yup.number().nullable().positive().integer(),
            maxTokensAfterSummarization: yup.number().nullable().positive().integer(),
        })
        .nullable(),

    subAgents: yup.array().of(
        yup.object({
            identifier: yup.string().required(),
            description: yup.string().required(),
            isEditing: yup.boolean(),
        })
    ),
});

function getIsToolNameUnique(value: string, ctx: yup.TestContext<EditAiAgentValidationContext>): boolean {
    const allActionNames = ctx.options.context.allActionNames ?? [];
    const allQueryNames = ctx.options.context.allQueryNames ?? [];
    const allToolNames = [...allActionNames, ...allQueryNames];

    const valuesCount = allToolNames.filter((name: string) => name === value).length;
    return valuesCount <= 1;
}

const testSchema = yup.object({
    prompt: yup.string().nullable().required(),
    parameters: yup.array().of(
        yup.object({
            name: yup.string().nullable(),
            value: aiAgentParametersUtils.createValueSchema(
                yup
                    .string()
                    .nullable()
                    .when("type", {
                        is: (type: AiAgentParameterValueType) => type !== "Null",
                        then: (schema) => schema.required(),
                    })
            ),
            type: yup.string<AiAgentParameterValueType>(),
            isSendToModel: yup.boolean(),
        })
    ),
});

export const editAiAgentYupResolver = yupResolver(editSchema);
export type EditAiAgentFormData = yup.InferType<typeof editSchema>;

export const testAiAgentYupResolver = yupResolver(testSchema);
export type TestAiAgentFormData = yup.InferType<typeof testSchema>;
