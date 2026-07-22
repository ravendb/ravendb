/* eslint-disable react-hooks/incompatible-library */
// React Compiler memoization is disabled here: useVirtualizer returns a mutable instance
// whose scroll-driven state must be re-read on every render.
"use no memo";

import { useRef, useState } from "react";
import { defaultRangeExtractor, useVirtualizer } from "@tanstack/react-virtual";
import { ChevronsDownUp, ChevronsUpDown, Plus } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildExplorerRows } from "@/pages/setup/add-app-wizard/steps/map-tables/build-explorer-rows";
import { ExplorerRowItem } from "@/pages/setup/add-app-wizard/steps/map-tables/explorer-rows";
import { getRootTablePath, type ExplorerRow } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { useTableActions } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-actions";

const SCHEMA_ROW_HEIGHT_PX = 24;
const TABLE_ROW_HEIGHT_PX = 32;

export function TablesExplorer() {
    const [filter, setFilter] = useState("");
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "mapTables.tables" }) ?? [];
    const expandedPaths = useSetupWizardStore((state) => state.mapExpandedPaths);
    const setAllMapTablesExpanded = useSetupWizardStore((state) => state.setAllMapTablesExpanded);
    const tableActions = useTableActions();

    const rows = buildExplorerRows({ tables, expandedPaths, filter });

    const handleSetAllExpanded = (isExpanded: boolean) => {
        setAllMapTablesExpanded(Object.fromEntries(tables.map((_, idx) => [getRootTablePath(idx), isExpanded])));
    };

    return (
        <div className="flex h-full min-h-0 flex-col gap-2 p-2">
            <div className="flex items-center gap-0.5">
                <div className="mr-auto px-1 text-sm font-medium">Tables</div>
                <Button
                    variant="ghost"
                    size="icon"
                    className="size-7"
                    title="Add new root table"
                    onClick={tableActions.addRootTable}
                >
                    <Plus className="size-4" aria-hidden="true" />
                </Button>
                <Button
                    variant="ghost"
                    size="icon"
                    className="size-7"
                    title="Collapse all tables"
                    onClick={() => handleSetAllExpanded(false)}
                >
                    <ChevronsDownUp className="size-4" aria-hidden="true" />
                </Button>
                <Button
                    variant="ghost"
                    size="icon"
                    className="size-7"
                    title="Expand all tables"
                    onClick={() => handleSetAllExpanded(true)}
                >
                    <ChevronsUpDown className="size-4" aria-hidden="true" />
                </Button>
            </div>
            <Input
                type="search"
                placeholder="Filter tables"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="h-8"
            />
            {tables.length === 0 ? (
                <EmptyExplorerMessage>No tables configured. Add a table to get started.</EmptyExplorerMessage>
            ) : rows.length === 0 ? (
                <EmptyExplorerMessage>No tables match the filter.</EmptyExplorerMessage>
            ) : (
                <VirtualizedExplorerRows rows={rows} />
            )}
        </div>
    );
}

function VirtualizedExplorerRows({ rows }: { rows: ExplorerRow[] }) {
    const scrollContainerRef = useRef<HTMLDivElement>(null);
    // Index of the schema header whose group is currently scrolled into view. It is kept
    // rendered and positioned sticky, replacing the CSS-only stickiness that plain document
    // flow gave before virtualization.
    const activeSchemaIndexRef = useRef(-1);

    const virtualizer = useVirtualizer({
        count: rows.length,
        getScrollElement: () => scrollContainerRef.current,
        estimateSize: (index) => (rows[index].type === "schema" ? SCHEMA_ROW_HEIGHT_PX : TABLE_ROW_HEIGHT_PX),
        getItemKey: (index) => rows[index].rowKey,
        overscan: 10,
        rangeExtractor: (range) => {
            activeSchemaIndexRef.current = findPrecedingSchemaIndex(rows, range.startIndex);
            const indexes = new Set(defaultRangeExtractor(range));

            if (activeSchemaIndexRef.current >= 0) {
                indexes.add(activeSchemaIndexRef.current);
            }

            return [...indexes].sort((a, b) => a - b);
        },
    });

    return (
        <div ref={scrollContainerRef} className="min-h-0 flex-1 overflow-y-auto">
            <div className="relative" style={{ height: virtualizer.getTotalSize() }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const isActiveSchemaHeader = virtualRow.index === activeSchemaIndexRef.current;

                    return (
                        <div
                            key={virtualRow.key}
                            className={cn("w-full", isActiveSchemaHeader ? "sticky z-10" : "absolute")}
                            style={{ height: virtualRow.size, top: isActiveSchemaHeader ? 0 : virtualRow.start }}
                        >
                            <ExplorerRowItem row={rows[virtualRow.index]} />
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

function findPrecedingSchemaIndex(rows: ExplorerRow[], fromIndex: number) {
    for (let index = Math.min(fromIndex, rows.length - 1); index >= 0; index--) {
        if (rows[index].type === "schema") {
            return index;
        }
    }

    return -1;
}

function EmptyExplorerMessage({ children }: { children: React.ReactNode }) {
    return <div className="px-2 py-6 text-center text-sm text-muted-foreground">{children}</div>;
}
