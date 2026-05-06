import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;
type CdcTaskState = Extract<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState, "Enabled" | "Disabled">;

const stringValueItemSchema = yup.object({
    value: yup.string(),
});

const cdcColumnMappingSchema = yup.object({
    column: yup.string(),
    name: yup.string(),
    type: yup.string<CdcColumnType>(),
});

const cdcSinkOnDeleteSchema = yup.object({
    ignoreDeletes: yup.boolean(),
    patch: yup.string().nullable(),
});

const cdcSinkLinkedTableSchema = yup.object({
    joinColumns: yup.array().of(stringValueItemSchema),
    linkedCollectionName: yup.string(),
    propertyName: yup.string(),
    sourceTableName: yup.string(),
    sourceTableSchema: yup.string().nullable(),
});

const cdcSinkEmbeddedTableBaseSchema = yup.object({
    caseSensitiveKeys: yup.boolean(),
    columns: yup.array().of(cdcColumnMappingSchema),
    joinColumns: yup.array().of(stringValueItemSchema),
    linkedTables: yup.array().of(cdcSinkLinkedTableSchema),
    onDelete: cdcSinkOnDeleteSchema,
    patch: yup.string().nullable(),
    primaryKeyColumns: yup.array().of(stringValueItemSchema),
    propertyName: yup.string(),
    sourceTableName: yup.string(),
    sourceTableSchema: yup.string().nullable(),
    type: yup.string<CdcSinkRelationType>(),
});

type CdcSinkEmbeddedTableFormData = yup.InferType<typeof cdcSinkEmbeddedTableBaseSchema> & {
    embeddedTables?: CdcSinkEmbeddedTableFormData[];
};

const getCdcSinkEmbeddedTableSchema = (): yup.ObjectSchema<CdcSinkEmbeddedTableFormData> =>
    cdcSinkEmbeddedTableBaseSchema.shape({
        embeddedTables: yup.array().of(yup.lazy(getCdcSinkEmbeddedTableSchema)),
    });

// TODO improve validation
const cdcSinkTableSchema = yup.object({
    collectionName: yup.string().required(),
    columns: yup.array().of(cdcColumnMappingSchema),
    disabled: yup.boolean(),
    embeddedTables: yup.array().of(getCdcSinkEmbeddedTableSchema()),
    linkedTables: yup.array().of(cdcSinkLinkedTableSchema),
    onDelete: cdcSinkOnDeleteSchema,
    patch: yup.string().nullable(),
    primaryKeyColumns: yup.array().of(stringValueItemSchema),
    sourceTableName: yup.string().required(),
    sourceTableSchema: yup.string().nullable(),
});

const editCdcSinkTaskSchema = yup.object({
    name: yup.string().nullable().required(),
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
