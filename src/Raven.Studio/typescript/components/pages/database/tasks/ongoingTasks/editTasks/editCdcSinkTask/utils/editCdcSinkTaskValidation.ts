import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;
type CdcTaskState = Extract<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState, "Enabled" | "Disabled">;

export interface EditCdcSinkTaskValidationContext {
    initialTaskName?: string;
    usedTaskNames: string[];
}

const cdcColumnMappingSchema = yup.object({
    Column: yup.string().required(),
    Name: yup.string().required(),
    Type: yup.string<CdcColumnType>().required(),
});

const cdcSinkOnDeleteSchema = yup.object({
    IgnoreDeletes: yup.boolean().required(),
    Patch: yup.string().nullable(),
});

const cdcSinkLinkedTableSchema = yup.object({
    JoinColumns: yup.array().of(yup.string().required()).required(),
    LinkedCollectionName: yup.string().required(),
    PropertyName: yup.string().required(),
    SourceTableName: yup.string().required(),
    SourceTableSchema: yup.string().nullable(),
});

const cdcSinkEmbeddedTableSchema = yup.object({
    CaseSensitiveKeys: yup.boolean().required(),
    Columns: yup.array().of(cdcColumnMappingSchema).required(),
    EmbeddedTables: yup
        .array()
        .of(yup.mixed<Raven.Client.Documents.Operations.CdcSink.CdcSinkEmbeddedTableConfig>())
        .required(),
    JoinColumns: yup.array().of(yup.string().required()).required(),
    LinkedTables: yup.array().of(cdcSinkLinkedTableSchema).required(),
    OnDelete: cdcSinkOnDeleteSchema.required(),
    Patch: yup.string().nullable(),
    PrimaryKeyColumns: yup.array().of(yup.string().required()).required(),
    PropertyName: yup.string().required(),
    SourceTableName: yup.string().required(),
    SourceTableSchema: yup.string().nullable(),
    Type: yup.string<CdcSinkRelationType>().required(),
});

const cdcSinkTableSchema = yup.object({
    CollectionName: yup.string().required(),
    Columns: yup.array().of(cdcColumnMappingSchema).required(),
    Disabled: yup.boolean().required(),
    EmbeddedTables: yup.array().of(cdcSinkEmbeddedTableSchema).required(),
    LinkedTables: yup.array().of(cdcSinkLinkedTableSchema).required(),
    OnDelete: cdcSinkOnDeleteSchema.required(),
    Patch: yup.string().nullable(),
    PrimaryKeyColumns: yup.array().of(yup.string().required()).required(),
    SourceTableName: yup.string().required(),
    SourceTableSchema: yup.string().nullable(),
});

const editCdcSinkTaskSchema = yup.object({
    name: yup
        .string()
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
    isSetResponsibleNode: yup.boolean().required(),
    responsibleNode: yup
        .string()
        .nullable()
        .when("isSetResponsibleNode", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isPinResponsibleNode: yup.boolean().required(),
    connectionStringName: yup.string().required(),
    skipInitialLoad: yup.boolean().required(),
    postgresPublicationName: yup.string().nullable(),
    postgresSlotName: yup.string().nullable(),

    // future sections
    tables: yup.array().of(cdcSinkTableSchema).required(),
});

export const editCdcSinkTaskResolver = yupResolver(editCdcSinkTaskSchema);
export type EditCdcSinkTaskFormData = yup.InferType<typeof editCdcSinkTaskSchema>;
