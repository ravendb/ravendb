import {
    encryptionStepSchema,
    dataDirectoryStepSchema,
    databaseNameSchema,
} from "../shared/createDatabaseSharedValidation";
import * as yup from "yup";

const basicInfoStepSchema = yup.object({
    databaseName: databaseNameSchema,
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
        .when(["$isManualReplication", "$isSharded"], {
            is: (isManualReplication: boolean, isSharded: boolean) => isManualReplication && isSharded,
            then: (schema) => schema.min(1),
        })
        .of(
            yup
                .array()
                .nullable()
                .of(yup.string().nullable())
                .test("at-least-one-replica", "Each shard needs at least one replica", (value, ctx) => {
                    if (!value || !ctx.options.context.isSharded || !ctx.options.context.isManualReplication) {
                        return true;
                    }
                    return value.some((x) => x);
                })
                .test(
                    "invalid-shard-topology",
                    "Invalid shard topology - replicas must reside on different nodes",
                    (value, ctx) => {
                        if (!value || !ctx.options.context.isSharded || !ctx.options.context.isManualReplication) {
                            return true;
                        }

                        const duplicates = value.filter(
                            (item, index) => item != null && item !== "None" && value.indexOf(item) != index
                        );
                        return duplicates.length === 0;
                    }
                )
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
