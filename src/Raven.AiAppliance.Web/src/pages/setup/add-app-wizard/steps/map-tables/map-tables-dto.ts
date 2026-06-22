import type {
    CdcColumnMapping,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkTableConfig,
} from "@/api/generated/server-api";
import { toStringValueItems, toStringValues } from "@/lib/form-utils";
import type {
    FormColumnMapping,
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

// The form stores string lists as { value } items (the FormStringList/useFieldArray shape),
// while the CdcSink* DTOs use plain string[]. These converters bridge the two shapes in both
// directions: form -> DTO when sending to the server (and exporting), DTO -> form when seeding
// the form from a server/AI suggestion (and importing).

export function mapFormTablesToDto(tables: FormRootTable[]): CdcSinkTableConfig[] {
    return tables.map(mapTableToDto);
}

function mapTableToDto(table: FormRootTable): CdcSinkTableConfig {
    return {
        collectionName: table.collectionName,
        columns: (table.columns ?? []).map(mapColumnToDto),
        disabled: table.disabled,
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        onDelete: mapOnDeleteToDto(table.onDelete),
        patch: table.patch || null,
        primaryKeyColumns: toStringValues(table.primaryKeyColumns),
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
    };
}

function mapEmbeddedTableToDto(table: FormEmbeddedTable): CdcSinkEmbeddedTableConfig {
    return {
        caseSensitiveKeys: table.caseSensitiveKeys,
        columns: (table.columns ?? []).map(mapColumnToDto),
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToDto),
        joinColumns: toStringValues(table.joinColumns),
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToDto),
        onDelete: mapOnDeleteToDto(table.onDelete),
        patch: table.patch || null,
        primaryKeyColumns: toStringValues(table.primaryKeyColumns),
        propertyName: table.propertyName,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
        type: table.type,
    };
}

function mapLinkedTableToDto(table: FormLinkedTable): CdcSinkLinkedTableConfig {
    return {
        joinColumns: toStringValues(table.joinColumns),
        linkedCollectionName: table.linkedCollectionName,
        propertyName: table.propertyName,
        sourceTableName: table.sourceTableName,
        sourceTableSchema: table.sourceTableSchema,
    };
}

function mapColumnToDto(column: FormColumnMapping): CdcColumnMapping {
    return {
        column: column.column,
        name: column.name,
        type: column.type,
    };
}

function mapOnDeleteToDto(onDelete: FormRootTable["onDelete"]): CdcSinkOnDeleteConfig {
    return {
        ignoreDeletes: onDelete?.ignoreDeletes ?? false,
        patch: onDelete?.patch || null,
    };
}

/** Converts DTO tables into the form's pre-validation shape. The result still needs
 * `tablesSchema.parse` to fill defaults and validate before it can be used as form data. */
export function wrapDtoTablesToFormShape(tables: CdcSinkTableConfig[]): unknown[] {
    return tables.map(wrapTableStringLists);
}

function wrapTableStringLists(table: CdcSinkTableConfig) {
    return {
        ...table,
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        embeddedTables: (table.embeddedTables ?? []).map(wrapEmbeddedTableStringLists),
        linkedTables: (table.linkedTables ?? []).map(wrapLinkedTableStringLists),
    };
}

function wrapEmbeddedTableStringLists(table: CdcSinkEmbeddedTableConfig): unknown {
    return {
        ...table,
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        joinColumns: toStringValueItems(table.joinColumns),
        embeddedTables: (table.embeddedTables ?? []).map(wrapEmbeddedTableStringLists),
        linkedTables: (table.linkedTables ?? []).map(wrapLinkedTableStringLists),
    };
}

function wrapLinkedTableStringLists(table: CdcSinkLinkedTableConfig) {
    return {
        ...table,
        joinColumns: toStringValueItems(table.joinColumns),
    };
}
