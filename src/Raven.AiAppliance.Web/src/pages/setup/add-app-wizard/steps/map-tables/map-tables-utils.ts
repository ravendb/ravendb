import type { DiscoverResponse, DiscoverTableResponse } from "@/api/generated/server-api";
import type {
    FormColumnMapping,
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import type { FieldErrors } from "react-hook-form";

export function pascalCase(value: string): string {
    return value
        .split(/[^a-zA-Z0-9]+/)
        .filter(Boolean)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join("");
}

export function propertyNameFromJoinColumn(column: string): string {
    return pascalCase(column.toLowerCase().endsWith("_id") ? column.slice(0, -3) : column);
}

export function getSourceTableLabel(table: { sourceTableSchema?: string | null; sourceTableName?: string | null }) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

export function createEmptyRootTable(): FormRootTable {
    return {
        collectionName: "",
        sourceTableSchema: null,
        sourceTableName: "",
        columns: [],
        primaryKeyColumns: [],
        patch: null,
        onDelete: { ignoreDeletes: false, patch: null },
        disabled: false,
        embeddedTables: [],
        linkedTables: [],
    };
}

export function createEmptyEmbeddedTable(): FormEmbeddedTable {
    return {
        sourceTableSchema: null,
        sourceTableName: "",
        propertyName: "",
        columns: [],
        primaryKeyColumns: [],
        joinColumns: [],
        type: "Array",
        patch: null,
        onDelete: { ignoreDeletes: false, patch: null },
        caseSensitiveKeys: false,
        embeddedTables: [],
        linkedTables: [],
    };
}

export function createEmptyLinkedTable(): FormLinkedTable {
    return {
        sourceTableSchema: null,
        sourceTableName: "",
        propertyName: "",
        joinColumns: [],
        linkedCollectionName: "",
    };
}

export function mapDiscoveredColumns(table: DiscoverTableResponse): FormColumnMapping[] {
    return table.columns
        .filter((column) => column.isCdcCapturable)
        .map((column) => ({
            column: column.name,
            name: pascalCase(column.name),
            type: column.suggestedType,
        }));
}

/** Scaffolds a root table from the discovered schema: columns and primary keys are
 * pre-filled, while embedding/linking decisions are left to the user. */
export function scaffoldRootTable(table: DiscoverTableResponse): FormRootTable {
    return {
        ...createEmptyRootTable(),
        collectionName: pascalCase(table.sourceTableName),
        sourceTableSchema: table.sourceTableSchema ?? null,
        sourceTableName: table.sourceTableName,
        columns: mapDiscoveredColumns(table),
        primaryKeyColumns: [...table.primaryKeyColumns],
    };
}

export function findDiscoveredTable(
    discoverResult: DiscoverResponse | null,
    sourceTableSchema: string | null | undefined,
    sourceTableName: string | null | undefined,
): DiscoverTableResponse | undefined {
    return discoverResult?.tables.find(
        (table) =>
            (table.sourceTableSchema ?? "") === (sourceTableSchema ?? "") && table.sourceTableName === sourceTableName,
    );
}

export function getDiscoveredSchemaNames(discoverResult: DiscoverResponse | null): string[] {
    const schemas = new Set(
        (discoverResult?.tables ?? [])
            .map((table) => table.sourceTableSchema)
            .filter((schema): schema is string => Boolean(schema)),
    );

    return [...schemas].sort();
}

export function getDiscoveredTableNames(
    discoverResult: DiscoverResponse | null,
    sourceTableSchema: string | null | undefined,
): string[] {
    const schemaFilter = sourceTableSchema?.trim();

    return (discoverResult?.tables ?? [])
        .filter((table) => !schemaFilter || table.sourceTableSchema === schemaFilter)
        .map((table) => table.sourceTableName)
        .sort();
}

export function getForeignKeysToTable(source: DiscoverTableResponse, target: DiscoverTableResponse) {
    return source.foreignKeys.filter(
        (foreignKey) =>
            (foreignKey.referencedSchema ?? "") === (target.sourceTableSchema ?? "") &&
            foreignKey.referencedTable === target.sourceTableName,
    );
}

/** Walks the form errors object along a dotted field path, e.g. "mapTables.tables.0". */
export function getErrorAtPath(errors: FieldErrors, path: string): unknown {
    let current: unknown = errors;

    for (const segment of path.split(".")) {
        if (current == null || typeof current !== "object") {
            return undefined;
        }
        current = (current as Record<string, unknown>)[segment];
    }

    return current;
}
