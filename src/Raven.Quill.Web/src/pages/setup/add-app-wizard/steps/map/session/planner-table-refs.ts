import type {
    CdcSinkEmbeddedTableConfig,
    CdcSinkTableConfig,
    MigrationFrameProposalFrame,
} from "@/api/generated/server-api";
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
export function resolveProposalTables(
    proposal: Omit<MigrationFrameProposalFrame, "type">,
    selectedTables: SelectedTables,
): PlannerProposal {
    const resolve = createTableResolver(selectedTables);

    return {
        areas: proposal.areas,
        collections: proposal.collections.map((proposed) => ({
            ...proposed,
            rootTableRef: resolve(proposed.rootTable),
            absorbs: proposed.absorbs.map((absorbed) => ({ ...absorbed, tableRef: resolve(absorbed.table) })),
        })),
        dropped: proposal.dropped.map((dropped) => ({ ...dropped, tableRef: resolve(dropped.table) })),
        enables: proposal.enables,
    };
}

export function resolveConfigTables(config: CdcSinkTableConfig, selectedTables: SelectedTables): ConfigTableRef {
    const resolve = createTableResolver(selectedTables);

    // An embedded table without a schema reads from its parent's, as the plan validator takes it.
    const resolveTable = (
        table: CdcSinkTableConfig | CdcSinkEmbeddedTableConfig,
        parentSchema: string | null,
    ): ConfigTableRef => {
        const schema = table.sourceTableSchema?.trim() || parentSchema;

        return {
            table: resolve(
                getTableLabel({ sourceTableSchema: schema, sourceTableName: table.sourceTableName?.trim() }),
            ),
            embeddedTables: (table.embeddedTables ?? []).map((embedded) => resolveTable(embedded, schema)),
        };
    };

    return resolveTable(config, null);
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
