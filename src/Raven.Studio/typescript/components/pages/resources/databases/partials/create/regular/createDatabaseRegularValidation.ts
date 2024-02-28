import { encryptionStepSchema, dataDirectoryStepSchema } from "../shared/createDatabaseSharedValidation";
import * as yup from "yup";

const basicInfoStepSchema = yup.object({
    databaseName: yup
        .string()
        .required()
        .when("$usedDatabaseNames", ([usedDatabaseNames], schema) =>
            schema.notOneOf(usedDatabaseNames, "Database already exists")
        ),

    isEncrypted: yup.boolean(),
});

const replicationAndShardingStepSchema = yup.object({
    replicationFactor: yup.number().integer().positive().required(),
    isSharded: yup.boolean(),
    shardsCount: yup
        .number()
        .nullable()
        .when("$isSharded", {
            is: true,
            then: (schema) => schema.integer().positive().max(100).required(),
        }),
    isDynamicDistribution: yup.boolean(),
    isManualReplication: yup.boolean(),
});

const manualNodeSelectionStepSchema = yup.object({
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
    basicInfoStep: basicInfoStepSchema,
    encryptionStep: encryptionStepSchema,
    replicationAndShardingStep: replicationAndShardingStepSchema,
    manualNodeSelectionStep: manualNodeSelectionStepSchema,
    dataDirectoryStep: dataDirectoryStepSchema,
});

export type CreateDatabaseRegularFormData = yup.InferType<typeof createDatabaseRegularSchema>;
