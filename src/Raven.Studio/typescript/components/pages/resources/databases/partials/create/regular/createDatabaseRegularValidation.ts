import * as yup from "yup";

const basicInfoSchema = yup.object({
    databaseName: yup.string().nullable().required(),
    isEncrypted: yup.boolean(),
});

const encryptionSchema = yup.object({
    isEncryptionKeySaved: yup.boolean().oneOf([true]),
});

const replicationAndShardingSchema = yup.object({
    replicationFactor: yup.number().integer().positive().required(),
    isSharded: yup.boolean(),
    shardsCount: yup
        .number()
        .nullable()
        .when("isSharded", {
            is: true,
            then: (schema) => schema.integer().positive().required(),
        }),
    isDynamicDistribution: yup.boolean(),
    isManualReplication: yup.boolean(),
});

const manualNodeSelectionSchema = yup.object({
    manualNodes: yup
        .array()
        .of(yup.string())
        .when("isManualReplication", {
            is: true,
            then: (schema) => schema.min(1),
        }),
    manualShard: yup.array().nullable().of(yup.array().of(yup.string())),
});

const pathsConfigurationsSchema = yup.object({
    isPathsConfigDefault: yup.boolean(),
    pathsConfig: yup
        .string()
        .nullable()
        .when("isPathsConfigDefault", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

export const createNewDatabaseSchema = yup
    .object()
    .concat(basicInfoSchema)
    .concat(encryptionSchema)
    .concat(replicationAndShardingSchema)
    .concat(manualNodeSelectionSchema)
    .concat(pathsConfigurationsSchema);

export type CreateNewDatabaseFormData = yup.InferType<typeof createNewDatabaseSchema>;
