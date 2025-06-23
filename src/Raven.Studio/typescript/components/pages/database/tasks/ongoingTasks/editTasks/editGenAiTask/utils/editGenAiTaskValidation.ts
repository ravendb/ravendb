import * as yup from "yup";

export type GenAiStartingPoint = "Beginning of Time" | "Latest Document" | "Change Vector";

export const editGenAiTaskSchema = yup.object({
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
        ),

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
        })
    ),
    playgroundModelOutputs: yup.array().of(yup.object({ idx: yup.number().nullable(), value: yup.string() })),
    isForceSendingCachedObjects: yup.boolean(),
});

export type EditGenAiTaskFormData = yup.InferType<typeof editGenAiTaskSchema>;
