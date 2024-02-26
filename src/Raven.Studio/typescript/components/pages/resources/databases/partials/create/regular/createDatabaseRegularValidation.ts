import { encryptionSchema, pathsConfigurationsSchema } from "../shared/createDatabaseSharedValidation";
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

const replicationAndShardingSchema = yup.object({
    replicationFactor: yup.number().integer().positive().required(),
    isSharded: yup.boolean(),
    shardsCount: yup
        .number()
        .nullable()
        .when("$isSharded", {
            is: true,
            then: (schema) => schema.integer().positive().required(),
        }),
    isDynamicDistribution: yup.boolean(),
    isManualReplication: yup.boolean(),
});

const manualNodeSelectionSchema = yup.object({
    nodes: yup
        .array()
        .of(yup.string())
        .when("$isManualReplication", {
            is: true,
            then: (schema) => schema.min(1),
        }),
    shards: yup
        .array()
        .nullable()
        .of(
            yup
                .array()
                .nullable()
                .of(yup.string().nullable())
                .test("at-least-one-replica", "Each shard needs at least one replica", (value) => {
                    if (!value) {
                        return true;
                    }
                    return value.some((x) => x);
                })
        ),
});

export const createDatabaseRegularSchema = yup.object({
    basicInfo: basicInfoSchema,
    encryption: encryptionSchema,
    replicationAndSharding: replicationAndShardingSchema,
    manualNodeSelection: manualNodeSelectionSchema,
    pathsConfigurations: pathsConfigurationsSchema,
});

export type CreateDatabaseRegularFormData = yup.InferType<typeof createDatabaseRegularSchema>;
