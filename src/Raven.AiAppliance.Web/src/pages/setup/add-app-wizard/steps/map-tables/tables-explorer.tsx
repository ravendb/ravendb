import { useState } from "react";
import { ChevronsDownUp, ChevronsUpDown, Plus } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildExplorerRows } from "@/pages/setup/add-app-wizard/steps/map-tables/build-explorer-rows";
import { ExplorerRowItem } from "@/pages/setup/add-app-wizard/steps/map-tables/explorer-rows";
import { getRootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { useTableActions } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-actions";

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
                <div className="min-h-0 flex-1 overflow-y-auto">
                    {rows.map((row) => (
                        <ExplorerRowItem key={row.rowKey} row={row} />
                    ))}
                </div>
            )}
        </div>
    );
}

function EmptyExplorerMessage({ children }: { children: React.ReactNode }) {
    return <div className="px-2 py-6 text-center text-sm text-muted-foreground">{children}</div>;
}
