import { yupResolver } from "@hookform/resolvers/yup";
import { RecursiveRequired } from "components/utils/common";
import { getDuplicateRootTableErrors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";
import { Resolver } from "react-hook-form/dist/types/resolvers";
import * as yup from "yup";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;
type CdcTaskState = Extract<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState, "Enabled" | "Disabled">;

const stringValueItemSchema = yup.object({
    value: yup.string().required(),
});

const cdcColumnMappingSchema = yup.object({
    column: yup.string().required(),
    name: yup.string().required(),
    type: yup.string<CdcColumnType>().required(),
});

const hasUniqueValues = <TItem>(items: TItem[], getValue: (item: TItem) => string) => {
    const values = (items ?? [])
        .map(getValue)
        .filter(Boolean)
        .map((x) => x.trim());

    return new Set(values).size === values.length;
};

const stringValueListSchema = (options: { uniqueMessage: string; requiredMessage?: string }) =>
    yup
        .array()
        .of(stringValueItemSchema)
        .min(options.requiredMessage ? 1 : 0, options.requiredMessage)
        .test("unique-values", options.uniqueMessage, (items) => hasUniqueValues(items, (item) => item.value));

const cdcColumnMappingsSchema = yup
    .array()
    .of(cdcColumnMappingSchema)
    .min(1, "At least one column mapping is required")
    .test("unique-source-columns", "Source columns must be unique", (items) =>
        hasUniqueValues(items, (item) => item.column)
    )
    .test("unique-target-columns", "Target columns must be unique", (items) =>
        hasUniqueValues(items, (item) => item.name)
    );

const cdcSinkOnDeleteSchema = yup.object({
    ignoreDeletes: yup.boolean(),
    patch: yup.string().nullable(),
});

const cdcSinkLinkedTableSchema = yup.object({
    joinColumns: stringValueListSchema({
        requiredMessage: "At least one join column is required",
        uniqueMessage: "Join columns must be unique",
    }),
    linkedCollectionName: yup.string().required(),
    propertyName: yup.string().required(),
    sourceTableName: yup.string().required(),
    sourceTableSchema: yup.string().nullable().required(),
});

const cdcSinkEmbeddedTableBaseSchema = yup.object({
    caseSensitiveKeys: yup.boolean(),
    columns: cdcColumnMappingsSchema,
    joinColumns: stringValueListSchema({
        requiredMessage: "At least one join column is required",
        uniqueMessage: "Join columns must be unique",
    }),
    linkedTables: yup.array().of(cdcSinkLinkedTableSchema),
    onDelete: cdcSinkOnDeleteSchema,
    patch: yup.string().nullable(),
    primaryKeyColumns: stringValueListSchema({
        requiredMessage: "At least one primary key column is required",
        uniqueMessage: "Primary key columns must be unique",
    }),
    propertyName: yup.string().required(),
    sourceTableName: yup.string().required(),
    sourceTableSchema: yup.string().nullable().required(),
    type: yup.string<CdcSinkRelationType>().required(),
});

type CdcSinkEmbeddedTableFormData = yup.InferType<typeof cdcSinkEmbeddedTableBaseSchema> & {
    embeddedTables?: CdcSinkEmbeddedTableFormData[];
};

const getCdcSinkEmbeddedTableSchema = (): yup.ObjectSchema<CdcSinkEmbeddedTableFormData> =>
    cdcSinkEmbeddedTableBaseSchema.shape({
        embeddedTables: yup.array().of(yup.lazy(getCdcSinkEmbeddedTableSchema)),
    });

const cdcSinkTableSchema = yup.object({
    collectionName: yup.string().required(),
    columns: cdcColumnMappingsSchema,
    disabled: yup.boolean(),
    embeddedTables: yup.array().of(getCdcSinkEmbeddedTableSchema()),
    linkedTables: yup.array().of(cdcSinkLinkedTableSchema),
    onDelete: cdcSinkOnDeleteSchema,
    patch: yup.string().nullable(),
    primaryKeyColumns: stringValueListSchema({
        requiredMessage: "At least one primary key column is required",
        uniqueMessage: "Primary key columns must be unique",
    }),
    sourceTableName: yup.string().required(),
    sourceTableSchema: yup.string().nullable().required(),
});

export const editCdcSinkTaskSchema = yup.object({
    name: yup.string().nullable().required(),
    state: yup.string<CdcTaskState>().required(),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup
        .string()
        .nullable()
        .when("isSetResponsibleNode", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isPinResponsibleNode: yup.boolean(),
    connectionStringName: yup.string().nullable().required(),
    skipInitialLoad: yup.boolean(),
    postgresPublicationName: yup.string().nullable(),
    postgresSlotName: yup.string().nullable(),
    tables: yup
        .array()
        .of(cdcSinkTableSchema)
        .min(1, "At least one table is required")
        .test("unique-collection-names", "Collection names must be unique", (items) =>
            hasUniqueValues(items, (item) => item.collectionName)
        )
        .test("unique-root-source-tables", function (items) {
            const duplicateErrors = getDuplicateRootTableErrors(items);

            if (duplicateErrors.length === 0) {
                return true;
            }

            return new yup.ValidationError(
                duplicateErrors.map(({ index, message }) =>
                    this.createError({
                        path: `tables.${index}.sourceTableName`,
                        message,
                    })
                )
            );
        }),
});

export type EditCdcSinkTaskFormData = RecursiveRequired<yup.InferType<typeof editCdcSinkTaskSchema>>;
export const editCdcSinkTaskResolver = yupResolver(editCdcSinkTaskSchema) as Resolver<EditCdcSinkTaskFormData>;
