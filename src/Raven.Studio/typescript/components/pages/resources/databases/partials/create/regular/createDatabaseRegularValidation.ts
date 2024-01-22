// import { useServices } from "components/hooks/useServices";
import { pathsConfigurationsSchema } from "../shared/createDatabaseSharedValidation";
import * as yup from "yup";

const basicInfoSchema = yup.object({
    databaseName: yup
        .string()
        .required()
        .when("$usedDatabaseNames", ([usedDatabaseNames], schema) =>
            schema.notOneOf(usedDatabaseNames, "Database already exists")
        ),

    isEncrypted: yup.boolean(),
});

const encryptionSchema = yup.object({
    isEncryptionKeySaved: yup.boolean().when("isEncrypted", {
        is: true,
        then: (schema) => schema.oneOf([true], "Encryption key must be saved"),
    }),
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

export const createDatabaseRegularSchema = yup
    .object()
    .concat(basicInfoSchema)
    .concat(encryptionSchema)
    .concat(replicationAndShardingSchema)
    .concat(manualNodeSelectionSchema)
    .concat(pathsConfigurationsSchema);

export type CreateDatabaseRegularFormData = yup.InferType<typeof createDatabaseRegularSchema>;
