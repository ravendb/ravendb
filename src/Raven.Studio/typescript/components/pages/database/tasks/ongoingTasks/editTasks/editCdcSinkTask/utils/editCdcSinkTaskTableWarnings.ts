import {
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";

export interface RootTablesAnalysis {
    sourceCountByKey: Map<string, number>;
    collectionNameKeysBySourceKey: Map<string, Set<string>>;
}

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

type RootTableAnalysisInput = SourceTableContext & {
    collectionName?: string;
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

export function getEmbeddedRootTableConflictWarning(rootTables: FormRootTable[], embeddedTable: SourceTableContext) {
    return getEmbeddedRootTableConflictWarningFromAnalysis(analyzeRootTables(rootTables), embeddedTable);
}

export function getMissingRelatedCollectionWarning(
    rootTables: FormRootTable[],
    linkedTable: LinkedTableWarningContext
) {
    return getMissingRelatedCollectionWarningFromAnalysis(analyzeRootTables(rootTables), linkedTable);
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

export function getMissingRelatedCollectionWarningFromAnalysis(
    analysis: RootTablesAnalysis,
    linkedTable: LinkedTableWarningContext
) {
    const linkedCollectionName = linkedTable.linkedCollectionName?.trim();
    const propertyName = linkedTable.propertyName?.trim();
    const sourceTableName = linkedTable.sourceTableName?.trim();
    const sourceTableSchema = linkedTable.sourceTableSchema?.trim();

    if (!linkedCollectionName || !sourceTableName || !sourceTableSchema || !propertyName) {
        return null;
    }

    const sourceKey = getSourceTableKey(sourceTableSchema, sourceTableName);
    const collectionNameKey = normalizeValue(linkedCollectionName);
    const collectionNameKeys = analysis.collectionNameKeysBySourceKey.get(sourceKey);
    const hasMatchingRootTable = collectionNameKeys?.has(collectionNameKey);

    if (hasMatchingRootTable) {
        return null;
    }

    return `No root table is configured for the related "${linkedCollectionName}" collection.
The ${propertyName} property will contain related document IDs that reference documents in the "${linkedCollectionName}" collection.
However, those documents will not be created unless "${sourceTableSchema}.${sourceTableName}" is also configured as a root table.`;
}

export function analyzeRootTables(rootTables: ReadonlyArray<RootTableAnalysisInput>): RootTablesAnalysis {
    const sourceCountByKey = new Map<string, number>();
    const collectionNameKeysBySourceKey = new Map<string, Set<string>>();

    (rootTables ?? []).forEach((rootTable) => {
        const sourceKey = getSourceTableKey(rootTable.sourceTableSchema, rootTable.sourceTableName);

        if (!sourceKey) {
            return;
        }

        sourceCountByKey.set(sourceKey, (sourceCountByKey.get(sourceKey) ?? 0) + 1);

        const collectionNameKey = normalizeValue(rootTable.collectionName);
        if (!collectionNameKey) {
            return;
        }

        if (!collectionNameKeysBySourceKey.has(sourceKey)) {
            collectionNameKeysBySourceKey.set(sourceKey, new Set<string>());
        }

        collectionNameKeysBySourceKey.get(sourceKey).add(collectionNameKey);
    });

    return {
        sourceCountByKey,
        collectionNameKeysBySourceKey,
    };
}

function getSourceTableKey(sourceTableSchema: string, sourceTableName: string) {
    const normalizedSchema = normalizeValue(sourceTableSchema);
    const normalizedTableName = normalizeValue(sourceTableName);

    if (!normalizedSchema || !normalizedTableName) {
        return null;
    }

    return `${normalizedSchema}::${normalizedTableName}`;
}

function getSourceTableLabel(table: SourceTableContext) {
    const sourceTableSchema = table.sourceTableSchema?.trim();
    const sourceTableName = table.sourceTableName?.trim();

    if (!sourceTableSchema || !sourceTableName) {
        return null;
    }

    return `"${sourceTableSchema}.${sourceTableName}"`;
}

function normalizeValue(value?: string) {
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
