import type {
    CdcColumnMapping,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkTableConfig,
} from "@/api/generated/server-api";

/**
 * Mirrors `CdcSinkTableConfig.ToJson()` in Raven.Client, so the mapping reads exactly the way RavenDB stores
 * it (and Studio shows it), while the Quill API speaks camelCase.
 */
export function toRavenDbTableJson(table: CdcSinkTableConfig) {
    return {
        CollectionName: table.collectionName ?? null,
        SourceTableSchema: table.sourceTableSchema ?? null,
        SourceTableName: table.sourceTableName ?? null,
        Columns: (table.columns ?? []).map(toColumnJson),
        PrimaryKeyColumns: table.primaryKeyColumns ?? [],
        Patch: table.patch ?? null,
        OnDelete: toOnDeleteJson(table.onDelete),
        Disabled: table.disabled ?? false,
        EmbeddedTables: (table.embeddedTables ?? []).map(toEmbeddedTableJson),
        LinkedTables: (table.linkedTables ?? []).map(toLinkedTableJson),
    };
}

function toEmbeddedTableJson(table: CdcSinkEmbeddedTableConfig): Record<string, unknown> {
    return {
        SourceTableSchema: table.sourceTableSchema ?? null,
        SourceTableName: table.sourceTableName ?? null,
        PropertyName: table.propertyName ?? null,
        Columns: (table.columns ?? []).map(toColumnJson),
        PrimaryKeyColumns: table.primaryKeyColumns ?? [],
        JoinColumns: table.joinColumns ?? [],
        Type: table.type ?? "Array",
        Patch: table.patch ?? null,
        OnDelete: toOnDeleteJson(table.onDelete),
        CaseSensitiveKeys: table.caseSensitiveKeys ?? false,
        EmbeddedTables: (table.embeddedTables ?? []).map(toEmbeddedTableJson),
        LinkedTables: (table.linkedTables ?? []).map(toLinkedTableJson),
    };
}

function toLinkedTableJson(table: CdcSinkLinkedTableConfig) {
    return {
        SourceTableSchema: table.sourceTableSchema ?? null,
        SourceTableName: table.sourceTableName ?? null,
        PropertyName: table.propertyName ?? null,
        JoinColumns: table.joinColumns ?? [],
        LinkedCollectionName: table.linkedCollectionName ?? null,
    };
}

/** RavenDB leaves `Type` out for a default column. */
function toColumnJson({ column, name, type = "Default" }: CdcColumnMapping) {
    if (type === "Default") {
        return { Column: column ?? null, Name: name ?? null };
    }

    return { Column: column ?? null, Name: name ?? null, Type: type };
}

function toOnDeleteJson(onDelete: CdcSinkOnDeleteConfig | undefined) {
    if (!onDelete) {
        return null;
    }

    return { Patch: onDelete.patch ?? null, IgnoreDeletes: onDelete.ignoreDeletes ?? false };
}
