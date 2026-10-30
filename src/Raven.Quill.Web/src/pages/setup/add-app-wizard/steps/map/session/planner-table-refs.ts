import type { CdcSinkEmbeddedTableConfig, CdcSinkTableConfig, MigrationProposal } from "@/api/generated/server-api";
import { getTableLabel, type SourceTableRef } from "@/pages/setup/add-app-wizard/discover-utils";
import type {
    ConfigTableRef,
    PlannerProposal,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/**
 * The planner writes table names as it likes - "dbo.Orders", "orders" - so each one is resolved to the
 * selected table it means. A name that fits more than one table is left unresolved rather than guessed.
 */
export function resolveProposalTables(proposal: MigrationProposal, selectedTables: SelectedTables): PlannerProposal {
    const resolve = createTableResolver(selectedTables);

    return {
        areas: proposal.areas.map((area) => ({
            ...area,
            collections: area.collections.map((proposed) => ({
                ...proposed,
                rootTableRef: resolve(proposed.rootTable),
                absorbs: proposed.absorbs.map((absorbed) => ({ ...absorbed, tableRef: resolve(absorbed.table) })),
            })),
        })),
        dropped: proposal.dropped.map((dropped) => ({ ...dropped, tableRef: resolve(dropped.table) })),
        enables: proposal.enables,
    };
}

/** A table without a schema reads from the provider's default schema, as CDC Sink resolves it. */
export function resolveConfigTables(
    config: CdcSinkTableConfig,
    selectedTables: SelectedTables,
    defaultSchema: string | null,
): ConfigTableRef {
    const resolve = createTableResolver(selectedTables);

    const resolveTable = (table: CdcSinkTableConfig | CdcSinkEmbeddedTableConfig): ConfigTableRef => ({
        table: resolve(
            getTableLabel({
                sourceTableSchema: table.sourceTableSchema?.trim() || defaultSchema,
                sourceTableName: table.sourceTableName?.trim(),
            }),
        ),
        embeddedTables: (table.embeddedTables ?? []).map(resolveTable),
    });

    return resolveTable(config);
}

function createTableResolver(selectedTables: SelectedTables) {
    return (name: string | null | undefined): SourceTableRef | null => {
        const wanted = name?.trim();

        if (!wanted) {
            return null;
        }

        return (
            findSingleMatch(selectedTables, wanted, getTableLabel) ??
            findSingleMatch(selectedTables, wanted, (table) => table.sourceTableName)
        );
    };
}

/** Where identifiers are case-sensitive, two tables can differ only in case; the exact spelling decides then. */
function findSingleMatch(
    tables: SelectedTables,
    wanted: string,
    spellingOf: (table: SourceTableRef) => string,
): SourceTableRef | null {
    const matches = tables.filter((table) => spellingOf(table).toLowerCase() === wanted.toLowerCase());
    const candidates = matches.length > 1 ? matches.filter((table) => spellingOf(table) === wanted) : matches;

    return candidates.length === 1 ? candidates[0] : null;
}
