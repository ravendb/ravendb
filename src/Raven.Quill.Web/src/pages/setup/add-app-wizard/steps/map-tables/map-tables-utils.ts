import type { DiscoverForeignKeyResponse, DiscoverResponse, DiscoverTableResponse } from "@/api/generated/server-api";
import { toStringValueItems } from "@/lib/form-utils";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { isColumnSupported, isTableSupported } from "@/pages/setup/add-app-wizard/discover-utils";
import type {
    FormColumnMapping,
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import type { FieldErrors } from "react-hook-form";

type SourceTableRef = { sourceTableSchema?: string | null; sourceTableName?: string | null };

export function pascalCase(value: string): string {
    return value
        .split(/[^a-zA-Z0-9]+/)
        .filter(Boolean)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join("");
}

function stripIdSuffix(column: string): string {
    const separated = /^(.*[^a-zA-Z0-9])id$/i.exec(column);

    if (separated) {
        return separated[1];
    }

    const camelCased = /^(.*[a-z0-9])(?:Id|ID)$/.exec(column);

    return camelCased ? camelCased[1] : column;
}

export function propertyNameFromJoinColumn(column: string): string {
    return pascalCase(stripIdSuffix(column)) || pascalCase(column);
}

export function getSourceTableLabel(table: SourceTableRef) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

/** Normalized identity of a source table, matching the case-insensitive comparison the
 * validation schema uses for duplicate root tables. Null when the table has no name yet. */
export function getSourceTableKey(table: SourceTableRef): string | null {
    const name = table.sourceTableName?.trim().toLowerCase();

    if (!name) {
        return null;
    }

    return `${table.sourceTableSchema?.trim().toLowerCase() ?? ""}::${name}`;
}

/** Collects the keys of every source table whose data is captured by the mapping: root
 * tables plus nested embedded tables. Linked tables don't count - a link only references
 * documents by id, so the linked table still needs its own root mapping. */
export function collectMappedSourceTableKeys(tables: FormRootTable[]): Set<string> {
    const keys = new Set<string>();

    const visit = (table: SourceTableRef & { embeddedTables?: FormEmbeddedTable[] }) => {
        const key = getSourceTableKey(table);

        if (key) {
            keys.add(key);
        }

        table.embeddedTables?.forEach(visit);
    };

    tables.forEach(visit);

    return keys;
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

export function mapDiscoveredColumns(
    discoverResult: DiscoverResponse | null,
    table: DiscoverTableResponse,
): FormColumnMapping[] {
    return table.columns
        .filter((column) => isColumnSupported(discoverResult, table, column))
        .map((column) => ({
            column: column.name,
            name: pascalCase(column.name),
            type: column.suggestedType,
        }));
}

/** Scaffolds a linked table from a foreign key of the parent table, pointing at the
 * referenced table's default collection name. */
export function scaffoldLinkedTable(foreignKey: DiscoverForeignKeyResponse): FormLinkedTable {
    return {
        sourceTableSchema: foreignKey.referencedSchema ?? null,
        sourceTableName: foreignKey.referencedTable,
        propertyName: foreignKey.columns.map(propertyNameFromJoinColumn).join("And"),
        joinColumns: toStringValueItems(foreignKey.columns),
        linkedCollectionName: pascalCase(foreignKey.referencedTable),
    };
}

function withUniquePropertyNames(linkedTables: FormLinkedTable[], columns: FormColumnMapping[]): FormLinkedTable[] {
    const takenNames = new Set(columns.map((column) => column.name.toLowerCase()));

    return linkedTables.map((linkedTable) => {
        let propertyName = linkedTable.propertyName;
        let suffix = 1;

        while (takenNames.has(propertyName.toLowerCase())) {
            suffix++;
            propertyName = `${linkedTable.propertyName}${suffix}`;
        }

        takenNames.add(propertyName.toLowerCase());

        return propertyName === linkedTable.propertyName ? linkedTable : { ...linkedTable, propertyName };
    });
}

/** Scaffolds a root table from the discovered schema: columns, primary keys, and linked
 * tables (one per foreign key) are pre-filled, while embedding decisions are left to the user. */
export function scaffoldRootTable(
    discoverResult: DiscoverResponse | null,
    table: DiscoverTableResponse,
): FormRootTable {
    const columns = mapDiscoveredColumns(discoverResult, table);

    return {
        ...createEmptyRootTable(),
        collectionName: pascalCase(table.sourceTableName),
        sourceTableSchema: table.sourceTableSchema ?? null,
        sourceTableName: table.sourceTableName,
        columns,
        primaryKeyColumns: toStringValueItems(table.primaryKeyColumns),
        linkedTables: withUniquePropertyNames(table.foreignKeys.map(scaffoldLinkedTable), columns),
    };
}

/** Scaffolds root tables for the selected source tables; tables missing from the discovered
 * schema still get a bare root table so the user can fill the mapping in manually. */
export function scaffoldTables(
    selectedTables: AppFormData["verifySchema"]["tables"],
    discoverResult: DiscoverResponse | null,
): FormRootTable[] {
    return selectedTables.map((selected) => {
        const discovered = findDiscoveredTable(discoverResult, selected.sourceTableSchema, selected.sourceTableName);

        if (discovered) {
            return scaffoldRootTable(discoverResult, discovered);
        }

        return {
            ...createEmptyRootTable(),
            collectionName: pascalCase(selected.sourceTableName),
            sourceTableSchema: selected.sourceTableSchema ?? null,
            sourceTableName: selected.sourceTableName,
        };
    });
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
            .filter((table) => isTableSupported(discoverResult, table))
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
        .filter((table) => isTableSupported(discoverResult, table))
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
