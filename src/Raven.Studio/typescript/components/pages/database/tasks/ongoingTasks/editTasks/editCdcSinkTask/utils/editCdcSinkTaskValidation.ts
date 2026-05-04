import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;
type CdcTaskState = Extract<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState, "Enabled" | "Disabled">;

import CdcSink = Raven.Client.Documents.Operations.CdcSink;
import { yupObjectSchema } from "components/utils/yupUtils";

export interface EditCdcSinkTaskValidationContext {
    initialTaskName?: string;
    usedTaskNames: string[];
}

const cdcColumnMappingSchema = yup.object({
    Column: yup.string(),
    Name: yup.string(),
    Type: yup.string<CdcColumnType>(),
});

const cdcSinkOnDeleteSchema = yup.object({
    IgnoreDeletes: yup.boolean(),
    Patch: yup.string().nullable(),
});

const cdcSinkLinkedTableSchema = yup.object({
    JoinColumns: yup.array().of(yup.string()),
    LinkedCollectionName: yup.string(),
    PropertyName: yup.string(),
    SourceTableName: yup.string(),
    SourceTableSchema: yup.string().nullable(),
});

const cdcSinkEmbeddedTableSchema = yupObjectSchema<CdcSink.CdcSinkEmbeddedTableConfig>({
    CaseSensitiveKeys: yup.boolean(),
    Columns: yup.array().of(cdcColumnMappingSchema),
    EmbeddedTables: yup.array().of(yup.mixed<CdcSink.CdcSinkEmbeddedTableConfig>()),
    JoinColumns: yup.array().of(yup.string()),
    LinkedTables: yup.array().of(cdcSinkLinkedTableSchema),
    OnDelete: cdcSinkOnDeleteSchema,
    Patch: yup.string().nullable(),
    PrimaryKeyColumns: yup.array().of(yup.string()),
    PropertyName: yup.string(),
    SourceTableName: yup.string(),
    SourceTableSchema: yup.string().nullable(),
    Type: yup.string<CdcSinkRelationType>(),
});

// TODO improve validation
const cdcSinkTableSchema = yup.object({
    CollectionName: yup.string(),
    Columns: yup.array().of(cdcColumnMappingSchema),
    Disabled: yup.boolean(),
    EmbeddedTables: yup.array().of(cdcSinkEmbeddedTableSchema),
    LinkedTables: yup.array().of(cdcSinkLinkedTableSchema),
    OnDelete: cdcSinkOnDeleteSchema,
    Patch: yup.string().nullable(),
    PrimaryKeyColumns: yup.array().of(yup.string()).nullable(),
    SourceTableName: yup.string(),
    SourceTableSchema: yup.string().nullable(),
});

const editCdcSinkTaskSchema = yup.object({
    name: yup
        .string()
        .nullable()
        .required()
        .test("unique-task-name", "Task name is already used", (value, ctx) => {
            if (!value) {
                return true;
            }

            const validationContext = ctx.options.context as EditCdcSinkTaskValidationContext;
            const initialTaskName = validationContext?.initialTaskName?.toLowerCase();

            return !validationContext?.usedTaskNames?.some((taskName) => {
                const normalizedTaskName = taskName.toLowerCase();

                if (normalizedTaskName === initialTaskName) {
                    return false;
                }

                return normalizedTaskName === value.toLowerCase();
            });
        }),
    state: yup.string<CdcTaskState>().required(),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup
        .string()
        .nullable()
        .when("isSetResponsibleNode", {
            is: true,
            then: (schema) => schema,
        }),
    isPinResponsibleNode: yup.boolean(),
    connectionStringName: yup.string().nullable().required(),
    skipInitialLoad: yup.boolean(),
    postgresPublicationName: yup.string().nullable(),
    postgresSlotName: yup.string().nullable(),
    tables: yup.array().of(cdcSinkTableSchema).min(1),
});

export const editCdcSinkTaskResolver = yupResolver(editCdcSinkTaskSchema);
export type EditCdcSinkTaskFormData = yup.InferType<typeof editCdcSinkTaskSchema>;
