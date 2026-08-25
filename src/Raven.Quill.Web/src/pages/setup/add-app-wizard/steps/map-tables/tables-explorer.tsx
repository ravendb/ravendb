/* eslint-disable react-hooks/incompatible-library */
// React Compiler memoization is disabled here: useVirtualizer returns a mutable instance
// whose scroll-driven state must be re-read on every render.
"use no memo";

import { useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ChevronsDownUp, ChevronsUpDown, Plus } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { Text } from "@/components/typography";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildExplorerRows } from "@/pages/setup/add-app-wizard/steps/map-tables/build-explorer-rows";
import { ExplorerRowItem, SchemaRow } from "@/pages/setup/add-app-wizard/steps/map-tables/explorer-rows";
import { getRootTablePath, type ExplorerRow } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { useTableActions } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-actions";

const SCHEMA_ROW_HEIGHT_PX = 24;
const TABLE_ROW_HEIGHT_PX = 32;

export function TablesExplorer() {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "mapTables.tables" }) ?? [];
    // Filter lives in the store so focusMapTable callers can clear it when the
    // focused table would otherwise stay filtered out.
    const filter = useSetupWizardStore((state) => state.mapTablesFilter);
    const setFilter = useSetupWizardStore((state) => state.setMapTablesFilter);
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
                <Text variant="label" as="div" className="mr-auto px-1">
                    Tables
                </Text>
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

    const virtualizer = useVirtualizer({
        count: rows.length,
        getScrollElement: () => scrollContainerRef.current,
        estimateSize: (index) => (rows[index].type === "schema" ? SCHEMA_ROW_HEIGHT_PX : TABLE_ROW_HEIGHT_PX),
        getItemKey: (index) => rows[index].rowKey,
        overscan: 10,
    });

    // The schema header of the group scrolled into view, rendered as a sticky overlay pinned
    // over the list. This replaces the CSS-only stickiness that plain document flow gave before
    // virtualization; when the group's own header row is at the top, the overlay covers it
    // pixel-for-pixel.
    const activeSchemaLabel = getActiveSchemaLabel(rows, virtualizer.range?.startIndex ?? 0);

    // Scroll the active table into view when focusMapTable is called (e.g. "Next" blocked
    // by a validation error in an off-screen table). Initializing the ref to the current id
    // ignores requests raised while the explorer was unmounted.
    const focusRequestId = useSetupWizardStore((state) => state.mapFocusRequestId);
    const handledFocusRequestIdRef = useRef(useSetupWizardStore.getState().mapFocusRequestId);

    useEffect(() => {
        if (focusRequestId === handledFocusRequestIdRef.current) {
            return;
        }

        handledFocusRequestIdRef.current = focusRequestId;
        const activePath = useSetupWizardStore.getState().mapActiveTable?.path;
        const index = rows.findIndex((row) => row.type !== "schema" && row.path === activePath);

        if (index >= 0) {
            virtualizer.scrollToIndex(index, { align: "center" });
        }
    });

    return (
        <div ref={scrollContainerRef} className="min-h-0 flex-1 overflow-y-auto">
            {activeSchemaLabel !== null && (
                // The negative margin keeps the overlay out of the flow so the list below
                // starts at the container top. bg-background fills the corner pixels the
                // schema row's rounded corners leave uncovered.
                <div
                    className="sticky top-0 z-10 bg-background"
                    style={{ height: SCHEMA_ROW_HEIGHT_PX, marginBottom: -SCHEMA_ROW_HEIGHT_PX }}
                >
                    <SchemaRow label={activeSchemaLabel} />
                </div>
            )}
            <div className="relative" style={{ height: virtualizer.getTotalSize() }}>
                {virtualizer.getVirtualItems().map((virtualRow) => (
                    <div
                        key={virtualRow.key}
                        className="absolute w-full"
                        style={{ height: virtualRow.size, top: virtualRow.start }}
                    >
                        <ExplorerRowItem row={rows[virtualRow.index]} />
                    </div>
                ))}
            </div>
        </div>
    );
}

function getActiveSchemaLabel(rows: ExplorerRow[], firstVisibleIndex: number): string | null {
    for (let index = Math.min(firstVisibleIndex, rows.length - 1); index >= 0; index--) {
        const row = rows[index];

        if (row.type === "schema") {
            return row.label;
        }
    }

    return null;
}

function EmptyExplorerMessage({ children }: { children: React.ReactNode }) {
    return (
        <Text variant="muted" as="div" className="px-2 py-6 text-center">
            {children}
        </Text>
    );
}
