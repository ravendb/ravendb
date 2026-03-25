import { yupResolver } from "@hookform/resolvers/yup";
import { yupObjectSchema } from "components/utils/yupUtils";
import * as yup from "yup";

export type GenAiStartingPoint = "Beginning of Time" | "Latest Document" | "Change Vector";

export interface EditGenAiTaskValidationContext {
    allQueryNames: string[];
}

const attachmentsSchema = yup.array().of(
    yupObjectSchema<Raven.Server.Documents.ETL.Providers.AI.AiAttachment>({
        Data: yup.string(),
        Name: yup.string(),
        RemoteStorageId: yup.string().nullable(),
        DownloadDurationInMs: yup.number(),
        Source: yup.string<Raven.Server.Documents.ETL.Providers.AI.AiAttachmentSource>(),
        Type: yup.string(),
    }).nullable()
);

const editGenAiTaskSchema = yup.object({
    // basic step
    name: yup.string().required(),
    identifier: yup.string(),
    state: yup.string<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>().required(),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup.string().nullable(),
    isPinResponsibleNode: yup.boolean(),
    connectionStringName: yup.string().required(),
    isAllowEtlOnNonEncryptedChannel: yup.boolean(),
    maxConcurrency: yup.number().nullable().min(1).positive().integer(),
    isStartingPoint: yup.boolean(),
    startingPointType: yup.string<GenAiStartingPoint>().nullable(),
    startingPointChangeVector: yup
        .string()
        .nullable()
        .when(["isStartingPoint", "startingPointType"], {
            is: (isStartingPoint: boolean, startingPointType: GenAiStartingPoint) =>
                isStartingPoint && startingPointType === "Change Vector",
            then: (schema) => schema.required(),
        }),
    nextBatchStartingPoint: yup.string().nullable(),

    // context step
    collectionName: yup.string().required(),
    script: yup.string().required(),

    // model step
    prompt: yup.string().required(),
    sampleObject: yup.string().nullable(),
    jsonSchema: yup
        .string()
        .nullable()
        .test(
            "sampleObjectOrJsonSchema",
            "Either 'Sample response object' or 'JSON schema' must be provided",
            function (_, { parent }) {
                return !!parent.sampleObject || !!parent.jsonSchema;
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
    queries: yup.array().of(
        yup.object({
            name: yup
                .string()
                .required()
                .matches(/^[a-zA-Z0-9_-]+$/, "Tool name can only contain letters, numbers, underscores and hyphens")
                .test(
                    "unique-name",
                    "Tool name must be unique",
                    (value: string, ctx: yup.TestContext<EditGenAiTaskValidationContext>) => {
                        const allQueryNames = ctx.options.context.allQueryNames ?? [];

                        const valuesCount = allQueryNames.filter((name: string) => name === value).length;
                        return valuesCount <= 1;
                    }
                ),
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
    isEnableTracing: yup.boolean(),
    isSetTracingExpiration: yup.boolean(),
    tracingExpirationInSeconds: yup.number().nullable().positive().integer(),

    // update step
    updateScript: yup.string().required(),

    // summary step
    isResetScript: yup.boolean(),
    scriptToReset: yup.string().nullable(),

    // playground
    documentId: yup.string(),
    playgroundDocument: yup.string(),
    playgroundContexts: yup.array().of(
        yup.object({
            idx: yup.number().nullable(),
            value: yup.string(),
            aiHash: yup.string(),
            isCached: yup.boolean(),
            attachments: attachmentsSchema,
        })
    ),
    playgroundModelOutputs: yup.array().of(
        yup.object({
            idx: yup.number().nullable(),
            value: yup.string(),
            attachments: attachmentsSchema,
        })
    ),
    isForceSendingCachedObjects: yup.boolean(),
});

export const editGenAiTaskResolver = yupResolver(editGenAiTaskSchema);
export type EditGenAiTaskFormData = yup.InferType<typeof editGenAiTaskSchema>;
export type GenAiAiAttachment = yup.InferType<typeof attachmentsSchema>[number];
