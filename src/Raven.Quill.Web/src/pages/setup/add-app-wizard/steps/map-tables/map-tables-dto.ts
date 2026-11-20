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

/** Fills values missing from the DTO (a hand-written or older configuration, or a server response
 * that omits nulls) with the defaults new tables get. The result still needs `tablesSchema` to validate. */
export function mapDtoTablesToForm(tables: CdcSinkTableConfig[]): FormRootTable[] {
    return tables.map(mapDtoTableToForm);
}

export function mapDtoTableToForm(table: CdcSinkTableConfig): FormRootTable {
    return {
        collectionName: table.collectionName ?? "",
        sourceTableSchema: table.sourceTableSchema ?? null,
        sourceTableName: table.sourceTableName ?? "",
        columns: (table.columns ?? []).map(mapColumnToForm),
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        patch: table.patch ?? null,
        onDelete: mapOnDeleteToForm(table.onDelete),
        disabled: table.disabled ?? false,
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToForm),
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToForm),
    };
}

function mapEmbeddedTableToForm(table: CdcSinkEmbeddedTableConfig): FormEmbeddedTable {
    return {
        sourceTableSchema: table.sourceTableSchema ?? null,
        sourceTableName: table.sourceTableName ?? "",
        propertyName: table.propertyName ?? "",
        columns: (table.columns ?? []).map(mapColumnToForm),
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        joinColumns: toStringValueItems(table.joinColumns),
        type: table.type ?? "Array",
        patch: table.patch ?? null,
        onDelete: mapOnDeleteToForm(table.onDelete),
        caseSensitiveKeys: table.caseSensitiveKeys ?? false,
        linkedTables: (table.linkedTables ?? []).map(mapLinkedTableToForm),
        embeddedTables: (table.embeddedTables ?? []).map(mapEmbeddedTableToForm),
    };
}

function mapLinkedTableToForm(table: CdcSinkLinkedTableConfig): FormLinkedTable {
    return {
        sourceTableSchema: table.sourceTableSchema ?? null,
        sourceTableName: table.sourceTableName ?? "",
        propertyName: table.propertyName ?? "",
        joinColumns: toStringValueItems(table.joinColumns),
        linkedCollectionName: table.linkedCollectionName ?? "",
    };
}

function mapColumnToForm(column: CdcColumnMapping): FormColumnMapping {
    return { column: column.column ?? "", name: column.name ?? "", type: column.type ?? "Default" };
}

function mapOnDeleteToForm(onDelete: CdcSinkOnDeleteConfig | null | undefined): FormRootTable["onDelete"] {
    return { patch: onDelete?.patch ?? null, ignoreDeletes: onDelete?.ignoreDeletes ?? false };
}
