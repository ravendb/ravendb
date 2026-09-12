import {
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";

export interface RootTablesAnalysis {
    // Disabled roots are counted too: replay and the test endpoint resolve them, so a duplicate
    // would shadow the other table's mapping even when disabled.
    sourceCountByKey: Map<string, number>;
    // Only enabled roots satisfy linked-table references — a disabled root creates no documents.
    enabledSourceKeys: Set<string>;
    // Normalized collection name -> collection name as the user typed it, per source table key.
    // Enabled roots only.
    collectionNamesBySourceKey: Map<string, Map<string, string>>;
    // Includes disabled roots' subtrees: the embedded/root conflict resurfaces on re-enable.
    embeddedSourceKeys: Set<string>;
    disabledSourceKeys: Set<string>;
}

type MissingRelatedCollectionIssue =
    | { type: "missingRootTable" }
    | { type: "disabledRootTable" }
    | { type: "collectionNameMismatch"; configuredCollectionNames: string[] };

interface SourceTableContext {
    sourceTableName?: string;
    sourceTableSchema?: string;
}

interface LinkedTableWarningContext {
    linkedCollectionName?: string;
    propertyName?: string;
    sourceTableName?: string;
    sourceTableSchema?: string;
}

interface DuplicateRootTableError {
    index: number;
    message: string;
}

type EmbeddedTableAnalysisInput = SourceTableContext & {
    embeddedTables?: ReadonlyArray<EmbeddedTableAnalysisInput>;
};

type RootTableAnalysisInput = SourceTableContext & {
    collectionName?: string;
    disabled?: boolean;
    embeddedTables?: ReadonlyArray<EmbeddedTableAnalysisInput>;
};

interface TableWarningContext {
    analysis: RootTablesAnalysis;
}

type TableWarningProvider<TTable> = (table: TTable, context: TableWarningContext) => string | null;

// Add new row warning rules here. They are collected for the row itself and bubbled up to parent/root rows.
const rootTableWarningProviders: TableWarningProvider<FormRootTable>[] = [];
const embeddedTableWarningProviders: TableWarningProvider<FormEmbeddedTable>[] = [
    (table, context) => getEmbeddedRootTableConflictWarningFromAnalysis(context.analysis, table),
];
const linkedTableWarningProviders: TableWarningProvider<FormLinkedTable>[] = [
    (table, context) => getMissingRelatedCollectionWarningFromAnalysis(context.analysis, table),
];

export function getDuplicateRootTableErrors(
    rootTables: ReadonlyArray<RootTableAnalysisInput>
): DuplicateRootTableError[] {
    const analysis = analyzeRootTables(rootTables);

    return (rootTables ?? []).flatMap((rootTable, index) => {
        const sourceTableLabel = getSourceTableLabel(rootTable);

        if (!sourceTableLabel) {
            return [];
        }

        const sourceKey = getSourceTableKey(rootTable.sourceTableSchema, rootTable.sourceTableName);
        const duplicateCount = analysis.sourceCountByKey.get(sourceKey) ?? 0;

        if (duplicateCount < 2) {
            return [];
        }

        return [
            {
                index,
                message: `Source table ${sourceTableLabel} is already configured as another root table. CDC Sink can process a source table only once.`,
            },
        ];
    });
}

export function getRootTableWarningMessagesFromAnalysis(analysis: RootTablesAnalysis, table?: FormRootTable) {
    if (!table) {
        return [];
    }

    const warnings = new Set<string>();
    collectRootTableWarningMessages(analysis, table, warnings);

    return Array.from(warnings);
}

export function getEmbeddedTableWarningMessagesFromAnalysis(analysis: RootTablesAnalysis, table?: FormEmbeddedTable) {
    if (!table) {
        return [];
    }

    const warnings = new Set<string>();
    collectEmbeddedTableWarningMessages(analysis, table, warnings);

    return Array.from(warnings);
}

export function getLinkedTableWarningMessagesFromAnalysis(analysis: RootTablesAnalysis, table?: FormLinkedTable) {
    if (!table) {
        return [];
    }

    const warnings = new Set<string>();
    collectLinkedTableWarningMessages(analysis, table, warnings);

    return Array.from(warnings);
}

export function getEmbeddedRootTableConflictWarningFromAnalysis(
    analysis: RootTablesAnalysis,
    embeddedTable: SourceTableContext
) {
    const sourceTableLabel = getSourceTableLabel(embeddedTable);

    if (!sourceTableLabel) {
        return null;
    }

    const sourceKey = getSourceTableKey(embeddedTable.sourceTableSchema, embeddedTable.sourceTableName);

    if (!analysis.sourceCountByKey.has(sourceKey)) {
        return null;
    }

    return `This source table is already configured as a root table.
CDC Sink can process a source table only once, so embedded updates may be routed to the root table instead.`;
}

function getMissingRelatedCollectionIssue(
    analysis: RootTablesAnalysis,
    linkedTable: LinkedTableWarningContext
): MissingRelatedCollectionIssue | null {
    const linkedCollectionName = linkedTable.linkedCollectionName?.trim();
    const propertyName = linkedTable.propertyName?.trim();
    const sourceKey = getSourceTableKey(linkedTable.sourceTableSchema, linkedTable.sourceTableName);

    if (!linkedCollectionName || !sourceKey || !propertyName) {
        return null;
    }

    if (!analysis.enabledSourceKeys.has(sourceKey)) {
        return analysis.disabledSourceKeys.has(sourceKey)
            ? { type: "disabledRootTable" }
            : { type: "missingRootTable" };
    }

    const configuredCollectionNames = analysis.collectionNamesBySourceKey.get(sourceKey);

    if (configuredCollectionNames?.has(normalizeValue(linkedCollectionName))) {
        return null;
    }

    return {
        type: "collectionNameMismatch",
        configuredCollectionNames: Array.from(configuredCollectionNames?.values() ?? []),
    };
}

export function getMissingRelatedCollectionWarningFromAnalysis(
    analysis: RootTablesAnalysis,
    linkedTable: LinkedTableWarningContext
) {
    const issue = getMissingRelatedCollectionIssue(analysis, linkedTable);

    if (!issue) {
        return null;
    }

    const linkedCollectionName = linkedTable.linkedCollectionName.trim();
    const sourceTableLabel = getSourceTableLabel(linkedTable);

    if (issue.type === "missingRootTable") {
        return `Related documents in the "${linkedCollectionName}" collection will not be created because ${sourceTableLabel} is not configured as a root table.`;
    }

    if (issue.type === "disabledRootTable") {
        return `Related documents in the "${linkedCollectionName}" collection will not be created because the ${sourceTableLabel} root table is disabled.`;
    }

    const configuredCollectionsLabel =
        issue.configuredCollectionNames.length > 0
            ? `targets ${formatCollectionNames(issue.configuredCollectionNames)} instead`
            : "targets a different collection";

    return `Related documents in the "${linkedCollectionName}" collection will not be created because the ${sourceTableLabel} root table ${configuredCollectionsLabel}.`;
}

function formatCollectionNames(collectionNames: string[]) {
    const quotedNames = collectionNames.map((name) => `"${name}"`).join(", ");
    return collectionNames.length === 1 ? `the ${quotedNames} collection` : `the ${quotedNames} collections`;
}

export function analyzeRootTables(rootTables: ReadonlyArray<RootTableAnalysisInput>): RootTablesAnalysis {
    const sourceCountByKey = new Map<string, number>();
    const enabledSourceKeys = new Set<string>();
    const collectionNamesBySourceKey = new Map<string, Map<string, string>>();
    const embeddedSourceKeys = new Set<string>();
    const disabledSourceKeys = new Set<string>();

    const collectEmbeddedSourceKeys = (embeddedTables?: ReadonlyArray<EmbeddedTableAnalysisInput>) => {
        (embeddedTables ?? []).forEach((embeddedTable) => {
            const key = getSourceTableKey(embeddedTable.sourceTableSchema, embeddedTable.sourceTableName);
            if (key) {
                embeddedSourceKeys.add(key);
            }

            collectEmbeddedSourceKeys(embeddedTable.embeddedTables);
        });
    };

    (rootTables ?? []).forEach((rootTable) => {
        collectEmbeddedSourceKeys(rootTable.embeddedTables);

        const sourceKey = getSourceTableKey(rootTable.sourceTableSchema, rootTable.sourceTableName);

        if (!sourceKey) {
            return;
        }

        sourceCountByKey.set(sourceKey, (sourceCountByKey.get(sourceKey) ?? 0) + 1);

        if (rootTable.disabled) {
            disabledSourceKeys.add(sourceKey);
            return;
        }

        enabledSourceKeys.add(sourceKey);

        const collectionName = rootTable.collectionName?.trim();
        const collectionNameKey = normalizeValue(collectionName);
        if (!collectionNameKey) {
            return;
        }

        if (!collectionNamesBySourceKey.has(sourceKey)) {
            collectionNamesBySourceKey.set(sourceKey, new Map<string, string>());
        }

        const collectionNames = collectionNamesBySourceKey.get(sourceKey);
        if (!collectionNames.has(collectionNameKey)) {
            collectionNames.set(collectionNameKey, collectionName);
        }
    });

    return {
        sourceCountByKey,
        enabledSourceKeys,
        collectionNamesBySourceKey,
        embeddedSourceKeys,
        disabledSourceKeys,
    };
}

// Case-insensitive identity of a source table; null when schema or table name is missing —
// unresolved entries must stay out of any matching.
export function getSourceTableKey(sourceTableSchema: string, sourceTableName: string) {
    const normalizedSchema = normalizeValue(sourceTableSchema);
    const normalizedTableName = normalizeValue(sourceTableName);

    if (!normalizedSchema || !normalizedTableName) {
        return null;
    }

    return `${normalizedSchema}::${normalizedTableName}`;
}

export function getSourceTableLabel(table: SourceTableContext) {
    const sourceTableSchema = table.sourceTableSchema?.trim();
    const sourceTableName = table.sourceTableName?.trim();

    if (!sourceTableSchema || !sourceTableName) {
        return null;
    }

    return `"${sourceTableSchema}.${sourceTableName}"`;
}

export function normalizeValue(value?: string) {
    return value?.trim().toLowerCase() ?? "";
}

function collectRootTableWarningMessages(analysis: RootTablesAnalysis, table: FormRootTable, warnings: Set<string>) {
    addWarningsFromProviders(warnings, rootTableWarningProviders, table, { analysis });
    collectChildTableWarningMessages(analysis, table, warnings);
}

function collectEmbeddedTableWarningMessages(
    analysis: RootTablesAnalysis,
    table: FormEmbeddedTable,
    warnings: Set<string>
) {
    addWarningsFromProviders(warnings, embeddedTableWarningProviders, table, { analysis });
    collectChildTableWarningMessages(analysis, table, warnings);
}

function collectLinkedTableWarningMessages(
    analysis: RootTablesAnalysis,
    table: FormLinkedTable,
    warnings: Set<string>
) {
    addWarningsFromProviders(warnings, linkedTableWarningProviders, table, { analysis });
}

function collectChildTableWarningMessages(
    analysis: RootTablesAnalysis,
    table: FormRootTable | FormEmbeddedTable,
    warnings: Set<string>
) {
    table?.linkedTables?.forEach((linkedTable) => {
        collectLinkedTableWarningMessages(analysis, linkedTable, warnings);
    });

    table?.embeddedTables?.forEach((embeddedTable) => {
        collectEmbeddedTableWarningMessages(analysis, embeddedTable, warnings);
    });
}

function addWarningsFromProviders<TTable>(
    warnings: Set<string>,
    providers: ReadonlyArray<TableWarningProvider<TTable>>,
    table: TTable,
    context: TableWarningContext
) {
    providers.forEach((provider) => {
        const warning = provider(table, context);

        if (warning) {
            warnings.add(warning);
        }
    });
}
