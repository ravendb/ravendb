import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";

const onDeleteSchema = yup
    .object({
        patch: yup.string().nullable(),
        ignoreDeletes: yup.boolean(),
    })
    .nullable()
    .default(null);

const columnMappingSchema = yup.object({
    column: yup.string().required("SQL column name is required"),
    name: yup.string().required("Target name is required"),
    type: yup.string<"Default" | "Json" | "Attachment">().required(),
});

const embeddedTableSchema: yup.Lazy<any> = yup.lazy(() =>
    yup.object({
        sourceTableSchema: yup.string().nullable(),
        sourceTableName: yup.string().required(),
        propertyName: yup.string().required(),
        type: yup.string<"Array" | "Map" | "Value">().required(),
        joinColumns: yup.array().of(yup.string().required()).min(1, "At least one join column is required"),
        primaryKeyColumns: yup.array().of(yup.string().required()),
        columns: yup.array().of(columnMappingSchema),
        patch: yup.string().nullable(),
        onDelete: onDeleteSchema,
        caseSensitiveKeys: yup.boolean(),
        embeddedTables: yup.array().of(yup.lazy(() => embeddedTableSchema)),
    })
);

const linkedTableSchema = yup.object({
    sourceTableSchema: yup.string().nullable(),
    sourceTableName: yup.string().required(),
    propertyName: yup.string().required(),
    linkedCollectionName: yup.string().required(),
    type: yup.string<"Array" | "Value">().required(),
    joinColumns: yup.array().of(yup.string().required()).min(1, "At least one join column is required"),
});

const tableSchema = yup.object({
    name: yup.string().required("Collection name is required"),
    sourceTableSchema: yup.string().nullable(),
    sourceTableName: yup.string().required("Source table name is required"),
    columns: yup
        .array()
        .of(columnMappingSchema)
        .min(1, "At least one column mapping is required"),
    primaryKeyColumns: yup
        .array()
        .of(yup.string().required())
        .min(1, "At least one primary key column is required"),
    patch: yup.string().nullable(),
    onDelete: onDeleteSchema,
    disabled: yup.boolean(),
    embeddedTables: yup.array().of(embeddedTableSchema),
    linkedTables: yup.array().of(linkedTableSchema),
});

const editCdcSinkTaskSchema = yup.object({
    name: yup.string().required("Task name is required"),
    connectionStringName: yup.string().required("Connection string is required"),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup.string().nullable(),
    disabled: yup.boolean(),
    tables: yup
        .array()
        .of(tableSchema)
        .min(1, "At least one table is required"),
});

export const editCdcSinkTaskResolver = yupResolver(editCdcSinkTaskSchema);
export type EditCdcSinkTaskFormData = yup.InferType<typeof editCdcSinkTaskSchema>;
