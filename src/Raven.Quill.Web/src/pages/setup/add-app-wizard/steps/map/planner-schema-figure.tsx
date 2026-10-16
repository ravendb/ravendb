import { Eye, Lock, Table } from "lucide-react";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableLabel } from "@/pages/setup/add-app-wizard/discover-utils";
import { findDiscoveredTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

const SHOWN_COLUMN_COUNT = 4;

/** Widths of the stand-in bars for the rows the planner never reads, in percent of a column. */
const HIDDEN_ROW_WIDTHS = [
    [38, 52, 78, 48],
    [30, 46, 70, 56],
    [42, 50, 74, 40],
];

export function PlannerSchemaFigure({ tables }: { tables: AppFormData["verifySchema"]["tables"] }) {
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);
    const [shownTable, ...otherTables] = tables;

    if (!shownTable) {
        return null;
    }

    const label = getTableLabel(shownTable);
    const columns =
        findDiscoveredTable(discoverResult, shownTable.sourceTableSchema, shownTable.sourceTableName)?.columns.slice(
            0,
            SHOWN_COLUMN_COUNT,
        ) ?? [];
    const [middleTable, backTable] = otherTables;

    return (
        <figure
            aria-label={
                tables.length === 1
                    ? `What the planner reads from ${label}: its columns and types, but not its rows`
                    : `What the planner reads from ${label}, one of the ${formatCount(tables.length, "table")}: its columns and types, but not its rows`
            }
            className="relative pt-11"
        >
            {backTable && (
                <StackedTable label={getTableLabel(backTable)} className="top-0 right-10 left-10 bg-surface2" />
            )}
            {middleTable && (
                <StackedTable label={getTableLabel(middleTable)} className="top-5.5 right-5 left-5 bg-card" />
            )}
            <div className="relative overflow-hidden rounded-xl border bg-card shadow-lifted">
                <div className="flex items-center gap-2 border-b px-4 py-2.75">
                    <Table className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
                    <Text as="span" variant="code" className="truncate">
                        {label}
                    </Text>
                    <Text variant="caption" as="span" className="ml-auto shrink-0">
                        1 of {formatCount(tables.length, "table")}
                    </Text>
                </div>

                <div className="flex flex-col gap-3 border-b border-brand-600/30 bg-primary/6 px-4 pt-3.5 pb-4 dark:border-primary/30 dark:bg-primary/7">
                    <Text as="span" variant="label" className="flex items-center gap-1.5 text-primary-strong">
                        <Eye className="size-3 shrink-0" aria-hidden="true" />
                        What the planner reads
                    </Text>
                    <div className="grid grid-cols-4 gap-3">
                        {columns.map((column) => (
                            <div key={column.name} className="flex min-w-0 flex-col gap-1">
                                <Text as="span" variant="code" className="truncate">
                                    {column.name}
                                </Text>
                                <Text as="span" variant="code" className="truncate text-muted-foreground">
                                    {column.nativeType}
                                </Text>
                            </div>
                        ))}
                    </div>
                </div>

                <div className="flex flex-col gap-2.5 px-4 pt-3.5 pb-4.5">
                    <Text as="span" variant="label" className="flex items-center gap-1.5 text-muted-foreground">
                        <Lock className="size-3 shrink-0" aria-hidden="true" />
                        Never read: the rows stay in your database
                    </Text>
                    <div aria-hidden="true" className="flex flex-col gap-2.5 pt-0.5">
                        {HIDDEN_ROW_WIDTHS.map((widths, rowIndex) => (
                            <div
                                key={widths.join("-")}
                                className={cn(
                                    "grid grid-cols-4 gap-3",
                                    rowIndex === HIDDEN_ROW_WIDTHS.length - 1 && "opacity-60",
                                )}
                            >
                                {widths.map((width, columnIndex) => (
                                    <span
                                        key={columnIndex}
                                        className="h-2 rounded-sm bg-muted"
                                        style={{ width: `${width}%` }}
                                    />
                                ))}
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </figure>
    );
}

function StackedTable({ label, className }: { label: string; className: string }) {
    return (
        <div aria-hidden="true" className={cn("absolute h-20 rounded-xl border px-3.5 py-0.5", className)}>
            <Text as="span" variant="code" className="text-muted-foreground">
                {label}
            </Text>
        </div>
    );
}
