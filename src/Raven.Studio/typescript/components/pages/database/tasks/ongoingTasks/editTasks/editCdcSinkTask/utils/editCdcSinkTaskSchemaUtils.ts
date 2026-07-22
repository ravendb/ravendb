import { SelectOption } from "components/common/select/Select";
import {
    analyzeRootTables,
    getSourceTableKey,
    normalizeValue,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";
import {
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
    FormRootTableColumn,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { camelCase, upperFirst } from "lodash";

import CdcSinkSchema = Raven.Client.Documents.Operations.CdcSink.Schema;

export type CdcSinkSourceSchema = CdcSinkSchema.CdcSinkSourceSchema;
export type CdcSinkSourceTable = CdcSinkSchema.CdcSinkSourceTable;
export type CdcSinkSourceForeignKey = CdcSinkSourceTable["ForeignKeys"][number];

export interface CdcSinkSourceTableOption extends SelectOption<string> {
    table?: CdcSinkSourceTable;
}

export const pascalCase = (name: string) => upperFirst(camelCase(name));

export const propertyNameFromJoinColumn = (name: string) => pascalCase(name.endsWith("_id") ? name.slice(0, -3) : name);

export function isTableSupported(schema: CdcSinkSourceSchema, table: CdcSinkSourceTable) {
    return schema?.Success && table.UnsupportedReason == null && (table.IsCdcEnabled || schema.HasPermissionToSetup);
}

export function isColumnSupported(
    schema: CdcSinkSourceSchema,
    table: CdcSinkSourceTable,
    column: CdcSinkSchema.CdcSinkSourceColumn
) {
    return column.IsCdcCapturable || (schema?.HasPermissionToSetup && !table.IsCdcEnabled);
}

export function mapSourceColumnsToFormData(
    schema: CdcSinkSourceSchema,
    table: CdcSinkSourceTable
): FormRootTableColumn[] {
    return table.Columns.filter((column) => isColumnSupported(schema, table, column)).map(
        (x): FormRootTableColumn => ({
            column: x.Name,
            name: pascalCase(x.Name),
            type: x.SuggestedType,
        })
    );
}

export function stringValues(values: string[]) {
    return values.map((value) => ({ value }));
}

export function getSourceTableOptionValue(table: CdcSinkSourceTable) {
    return table.SourceTableName;
}

export function getSourceTableOptionLabel(table: CdcSinkSourceTable) {
    return `${table.SourceTableSchema}.${table.SourceTableName}`;
}

export function getSourceTableOptions(
    schema: CdcSinkSourceSchema,
    sourceTableSchema: string,
    excludedTable?: { sourceTableSchema: string; sourceTableName: string }
): CdcSinkSourceTableOption[] {
    const schemaFilter = normalizeValue(sourceTableSchema);
    const excludedKey = excludedTable
        ? getSourceTableKey(excludedTable.sourceTableSchema, excludedTable.sourceTableName)
        : null;

    return (schema?.Tables ?? [])
        .filter((table) => isTableSupported(schema, table))
        .filter((table) => !schemaFilter || normalizeValue(table.SourceTableSchema) === schemaFilter)
        .filter(
            (table) => !excludedKey || getSourceTableKey(table.SourceTableSchema, table.SourceTableName) !== excludedKey
        )
        .map((table) => ({
            value: getSourceTableOptionValue(table),
            label: getSourceTableOptionLabel(table),
            table,
        }));
}

export function getSourceSchemaOptions(schema: CdcSinkSourceSchema): SelectOption<string>[] {
    const schemas = new Set(
        (schema?.Tables ?? [])
            .filter((table) => isTableSupported(schema, table))
            .map((table) => table.SourceTableSchema)
    );

    return Array.from(schemas)
        .sort()
        .map((sourceSchema) => ({
            value: sourceSchema,
            label: sourceSchema,
        }));
}

export function findSourceTable(schema: CdcSinkSourceSchema, sourceTableSchema: string, sourceTableName: string) {
    const key = getSourceTableKey(sourceTableSchema, sourceTableName);

    if (!key) {
        return undefined;
    }

    return (schema?.Tables ?? []).find(
        (table) => getSourceTableKey(table.SourceTableSchema, table.SourceTableName) === key
    );
}

export function getForeignKeysToTable(sourceTable: CdcSinkSourceTable, targetTable: CdcSinkSourceTable) {
    const targetKey = getSourceTableKey(targetTable.SourceTableSchema, targetTable.SourceTableName);

    if (!targetKey) {
        return [];
    }

    return (sourceTable?.ForeignKeys ?? []).filter(
        (foreignKey) => getSourceTableKey(foreignKey.ReferencedSchema, foreignKey.ReferencedTable) === targetKey
    );
}

interface SourceTableReference {
    sourceTableSchema?: string;
    sourceTableName?: string;
    linkedCollectionName?: string;
}

function buildSourceTableLookup(schema: CdcSinkSourceSchema): Map<string, CdcSinkSourceTable> {
    const lookup = new Map<string, CdcSinkSourceTable>();

    (schema?.Tables ?? []).forEach((table) => {
        const key = getSourceTableKey(table.SourceTableSchema, table.SourceTableName);
        if (key) {
            lookup.set(key, table);
        }
    });

    return lookup;
}

function collectLinkedSourceReferences(table: FormRootTable | FormEmbeddedTable): SourceTableReference[] {
    const references: SourceTableReference[] = [];

    (table?.linkedTables ?? []).forEach((linkedTable) => {
        references.push({
            sourceTableSchema: linkedTable.sourceTableSchema,
            sourceTableName: linkedTable.sourceTableName,
            linkedCollectionName: linkedTable.linkedCollectionName,
        });
    });

    (table?.embeddedTables ?? []).forEach((embeddedTable) => {
        references.push(...collectLinkedSourceReferences(embeddedTable));
    });

    return references;
}

// Resolves the source tables to add as roots so every linked-table reference points at a
// configured root table. Follows foreign keys transitively (an added table maps them as linked
// tables); skips tables already configured as roots or embedded tables and unsupported or
// undiscovered tables — those stay per-link warnings.
export function getRelatedSourceTablesToAdd(
    schema: CdcSinkSourceSchema,
    rootTables: FormRootTable[]
): CdcSinkSourceTable[] {
    if (!schema?.Tables?.length) {
        return [];
    }

    const pendingReferences = (rootTables ?? [])
        .filter((rootTable) => !rootTable?.disabled)
        .flatMap(collectLinkedSourceReferences);

    if (pendingReferences.length === 0) {
        return [];
    }

    const analysis = analyzeRootTables(rootTables ?? []);
    const lookup = buildSourceTableLookup(schema);
    const resolvedKeys = new Set(analysis.sourceCountByKey.keys());
    const tablesToAdd: CdcSinkSourceTable[] = [];

    // The queue grows while it is being consumed: each added table enqueues its own references.
    for (let index = 0; index < pendingReferences.length; index++) {
        const reference = pendingReferences[index];
        const key = getSourceTableKey(reference.sourceTableSchema, reference.sourceTableName);

        if (!key || resolvedKeys.has(key) || analysis.embeddedSourceKeys.has(key)) {
            continue;
        }

        // Marked resolved even when unaddable, so a reference is examined only once.
        resolvedKeys.add(key);

        const sourceTable = lookup.get(key);

        if (!sourceTable || !isTableSupported(schema, sourceTable)) {
            continue;
        }

        tablesToAdd.push(sourceTable);

        (sourceTable.ForeignKeys ?? []).forEach((foreignKey) => {
            pendingReferences.push({
                sourceTableSchema: foreignKey.ReferencedSchema,
                sourceTableName: foreignKey.ReferencedTable,
            });
        });
    }

    return tablesToAdd;
}

export function mapSqlTableToFormData(schema: CdcSinkSourceSchema, table: CdcSinkSourceTable): FormRootTable {
    const linkedTables = table.ForeignKeys.map(
        (x): FormLinkedTable => ({
            propertyName: x.Columns.map(propertyNameFromJoinColumn).join("And"),
            joinColumns: stringValues(x.Columns),
            linkedCollectionName: pascalCase(x.ReferencedTable),
            sourceTableName: x.ReferencedTable,
            sourceTableSchema: x.ReferencedSchema,
        })
    );

    return {
        collectionName: pascalCase(table.SourceTableName),
        columns: mapSourceColumnsToFormData(schema, table),
        disabled: false,
        embeddedTables: [],
        linkedTables,
        onDelete: { ignoreDeletes: false, patch: "" },
        patch: "",
        primaryKeyColumns: stringValues(table.PrimaryKeyColumns),
        sourceTableName: table.SourceTableName,
        sourceTableSchema: table.SourceTableSchema,
    } satisfies FormRootTable;
}

// Unlike a regular add, collection names adopt the name the referencing linked tables already
// use (de-duplicated against configured collections) and links point at the collections their
// target roots actually use — so the add cannot introduce a name clash or mismatch.
export function mapRelatedSqlTablesToFormData(
    schema: CdcSinkSourceSchema,
    rootTables: FormRootTable[],
    tablesToAdd: CdcSinkSourceTable[]
): FormRootTable[] {
    const analysis = analyzeRootTables(rootTables ?? []);
    const referencedNamesBySourceKey = collectReferencedCollectionNames(rootTables ?? []);

    const takenCollectionNames = new Set<string>();
    (rootTables ?? []).forEach((rootTable) => {
        const nameKey = normalizeValue(rootTable.collectionName);
        if (nameKey) {
            takenCollectionNames.add(nameKey);
        }
    });

    const collectionNameBySourceKey = new Map<string, string>();
    analysis.collectionNamesBySourceKey.forEach((names, key) => {
        const [firstConfiguredName] = names.values();
        if (firstConfiguredName) {
            collectionNameBySourceKey.set(key, firstConfiguredName);
        }
    });

    const formTables = (tablesToAdd ?? []).map((table) => {
        const formTable = mapSqlTableToFormData(schema, table);
        const key = getSourceTableKey(table.SourceTableSchema, table.SourceTableName);

        const referencedNames = key ? referencedNamesBySourceKey.get(key) : null;
        const [singleReferencedName] = referencedNames?.size === 1 ? referencedNames.values() : [];
        const collectionName = toUniqueCollectionName(
            singleReferencedName || formTable.collectionName,
            table,
            takenCollectionNames
        );

        formTable.collectionName = collectionName;
        takenCollectionNames.add(normalizeValue(collectionName));

        if (key && !collectionNameBySourceKey.has(key)) {
            collectionNameBySourceKey.set(key, collectionName);
        }

        return formTable;
    });

    // Second pass, so links between batch tables resolve regardless of order.
    formTables.forEach((formTable) => {
        formTable.linkedTables = formTable.linkedTables.map((linkedTable) => {
            const key = getSourceTableKey(linkedTable.sourceTableSchema, linkedTable.sourceTableName);
            const collectionName = key ? collectionNameBySourceKey.get(key) : null;

            return collectionName ? { ...linkedTable, linkedCollectionName: collectionName } : linkedTable;
        });
    });

    return formTables;
}

// Keep already configured links in sync when a newly added root table cannot use the collection
// name they originally requested (for example because that collection name is already taken).
export function alignLinkedTableCollectionNames(
    rootTables: FormRootTable[],
    targetRootTables: FormRootTable[]
): FormRootTable[] {
    const collectionNameBySourceKey = new Map<string, string>();

    (targetRootTables ?? []).forEach((rootTable) => {
        const key = getSourceTableKey(rootTable.sourceTableSchema, rootTable.sourceTableName);
        const collectionName = rootTable.collectionName?.trim();

        if (key && collectionName) {
            collectionNameBySourceKey.set(key, collectionName);
        }
    });

    if (collectionNameBySourceKey.size === 0) {
        return rootTables;
    }

    const alignTable = <TTable extends FormRootTable | FormEmbeddedTable>(table: TTable): TTable => {
        let isChanged = false;

        const linkedTables = (table.linkedTables ?? []).map((linkedTable) => {
            const key = getSourceTableKey(linkedTable.sourceTableSchema, linkedTable.sourceTableName);
            const collectionName = key ? collectionNameBySourceKey.get(key) : null;

            if (
                !collectionName ||
                normalizeValue(linkedTable.linkedCollectionName) === normalizeValue(collectionName)
            ) {
                return linkedTable;
            }

            isChanged = true;
            return { ...linkedTable, linkedCollectionName: collectionName };
        });

        const embeddedTables = (table.embeddedTables ?? []).map((embeddedTable) => {
            const alignedTable = alignTable(embeddedTable);
            isChanged ||= alignedTable !== embeddedTable;
            return alignedTable;
        });

        return isChanged ? ({ ...table, linkedTables, embeddedTables } as TTable) : table;
    };

    return (rootTables ?? []).map(alignTable);
}

// Normalized collection name -> collection name as the user typed it, per referenced source table
// key. Disabled roots are skipped, matching getRelatedSourceTablesToAdd.
function collectReferencedCollectionNames(rootTables: FormRootTable[]): Map<string, Map<string, string>> {
    const namesBySourceKey = new Map<string, Map<string, string>>();

    rootTables
        .filter((rootTable) => !rootTable?.disabled)
        .flatMap(collectLinkedSourceReferences)
        .forEach((reference) => {
            const key = getSourceTableKey(reference.sourceTableSchema, reference.sourceTableName);
            const name = reference.linkedCollectionName?.trim();
            const nameKey = normalizeValue(name);

            if (!key || !nameKey) {
                return;
            }

            if (!namesBySourceKey.has(key)) {
                namesBySourceKey.set(key, new Map<string, string>());
            }

            const names = namesBySourceKey.get(key);
            if (!names.has(nameKey)) {
                names.set(nameKey, name);
            }
        });

    return namesBySourceKey;
}

function toUniqueCollectionName(preferredName: string, table: CdcSinkSourceTable, takenNames: Set<string>) {
    const candidates = [preferredName, `${pascalCase(table.SourceTableSchema)}${preferredName}`];

    for (const candidate of candidates) {
        if (candidate && !takenNames.has(normalizeValue(candidate))) {
            return candidate;
        }
    }

    for (let suffix = 2; ; suffix++) {
        const candidate = `${preferredName}${suffix}`;

        if (!takenNames.has(normalizeValue(candidate))) {
            return candidate;
        }
    }
}
