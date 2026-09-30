// React Compiler memoization is disabled here: row error indicators are derived from
// react-hook-form's mutable errors object, which keeps a stable identity across updates.
"use no memo";

import { ArrowRight, ChevronDown, ChevronRight, CircleAlert, EllipsisVertical, Layers, Link2 } from "lucide-react";
import { useFormContext, useFormState, type FieldPath } from "react-hook-form";
import { Text } from "@/components/typography";
import { Button } from "@/components/shadcn/ui/button";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type {
    ExplorerRow,
    ExplorerRowEmbeddedTable,
    ExplorerRowLinkedTable,
    ExplorerRowRootTable,
    MapActiveTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { getErrorAtPath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useTableActions } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-actions";

const NESTED_TABLE_INDENT_PX = 14;

export function ExplorerRowItem({ row }: { row: ExplorerRow }) {
    switch (row.type) {
        case "schema":
            return <SchemaRow label={row.label} />;
        case "root":
            return <RootTableRow row={row} />;
        case "embedded":
            return <EmbeddedTableRow row={row} />;
        case "linked":
            return <LinkedTableRow row={row} />;
    }
}

export function SchemaRow({ label }: { label: string }) {
    // The vertical padding sits outside the filled band: it separates the header from the
    // previous group above and from its own first row below. Its height is part of
    // SCHEMA_ROW_HEIGHT_PX in the explorer.
    return (
        <div className="py-1">
            <Text variant="caption" as="div" className="rounded-sm bg-muted px-1.5 py-1 text-center font-mono">
                {label}
            </Text>
        </div>
    );
}

function RootTableRow({ row }: { row: ExplorerRowRootTable }) {
    const tableActions = useTableActions();
    const isDisabled = Boolean(row.table.disabled);

    return (
        <TableRowFrame
            row={row}
            depth={0}
            isDimmed={isDisabled}
            actions={
                <>
                    <DropdownMenuItem onSelect={() => tableActions.addEmbeddedTable(row.path)}>
                        <Layers aria-hidden="true" /> Add embedded table
                    </DropdownMenuItem>
                    <DropdownMenuItem onSelect={() => tableActions.addLinkedTable(row.path)}>
                        <Link2 aria-hidden="true" /> Add linked table
                    </DropdownMenuItem>
                    <DropdownMenuItem onSelect={() => tableActions.toggleRootTableDisabled(row.path)}>
                        {isDisabled ? "Enable" : "Disable"}
                    </DropdownMenuItem>
                    <DropdownMenuItem variant="destructive" onSelect={() => tableActions.removeTable(row.path)}>
                        Remove
                    </DropdownMenuItem>
                </>
            }
        />
    );
}

function EmbeddedTableRow({ row }: { row: ExplorerRowEmbeddedTable }) {
    const tableActions = useTableActions();

    return (
        <TableRowFrame
            row={row}
            depth={row.depth}
            isDimmed={row.isRootDisabled}
            typeIcon={<Layers className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />}
            actions={
                <>
                    <DropdownMenuItem onSelect={() => tableActions.addEmbeddedTable(row.path)}>
                        <Layers aria-hidden="true" /> Add embedded table
                    </DropdownMenuItem>
                    <DropdownMenuItem onSelect={() => tableActions.addLinkedTable(row.path)}>
                        <Link2 aria-hidden="true" /> Add linked table
                    </DropdownMenuItem>
                    <DropdownMenuItem variant="destructive" onSelect={() => tableActions.removeTable(row.path)}>
                        Remove
                    </DropdownMenuItem>
                </>
            }
        />
    );
}

function LinkedTableRow({ row }: { row: ExplorerRowLinkedTable }) {
    const tableActions = useTableActions();

    return (
        <TableRowFrame
            row={row}
            depth={row.depth}
            isDimmed={row.isRootDisabled}
            typeIcon={<Link2 className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />}
            actions={
                <DropdownMenuItem variant="destructive" onSelect={() => tableActions.removeTable(row.path)}>
                    Remove
                </DropdownMenuItem>
            }
        />
    );
}

type TableRowFrameProps = {
    row: ExplorerRowRootTable | ExplorerRowEmbeddedTable | ExplorerRowLinkedTable;
    depth: number;
    isDimmed: boolean;
    typeIcon?: React.ReactNode;
    actions: React.ReactNode;
};

function TableRowFrame({ row, depth, isDimmed, typeIcon, actions }: TableRowFrameProps) {
    const mapActiveTable = useSetupWizardStore((state) => state.mapActiveTable);
    const setMapActiveTable = useSetupWizardStore((state) => state.setMapActiveTable);
    const toggleMapTableExpanded = useSetupWizardStore((state) => state.toggleMapTableExpanded);
    const { control } = useFormContext<AppFormData>();
    const { errors } = useFormState({ control, name: row.path as FieldPath<AppFormData> });

    const isActive = mapActiveTable?.path === row.path;
    const hasError = Boolean(getErrorAtPath(errors, row.path));
    const expandableRow = row.type !== "linked" && row.hasChildren ? row : null;
    const label = row.table.sourceTableName || "Unassigned table";
    const collectionName = row.type === "root" ? row.table.collectionName : null;

    return (
        // The frame stays taller than its inner controls so the rounded row highlights get a
        // visible gap between them, while the frames themselves stay flush to keep the nested
        // border-l connector line unbroken. Its height is TABLE_ROW_HEIGHT_PX in the explorer.
        <div
            className={cn("flex h-9 items-center", depth > 0 && "border-l")}
            style={{ marginLeft: depth * NESTED_TABLE_INDENT_PX }}
        >
            {expandableRow ? (
                <Button
                    variant="ghost"
                    size="icon"
                    className="size-6 shrink-0"
                    title={expandableRow.isExpanded ? "Collapse table" : "Expand table"}
                    onClick={() => toggleMapTableExpanded(expandableRow.path)}
                >
                    {expandableRow.isExpanded ? (
                        <ChevronDown className="size-3.5" aria-hidden="true" />
                    ) : (
                        <ChevronRight className="size-3.5" aria-hidden="true" />
                    )}
                </Button>
            ) : (
                <span className="w-6 shrink-0" />
            )}
            <button
                type="button"
                onClick={() => setMapActiveTable({ type: row.type, path: row.path } as MapActiveTable)}
                title={collectionName ? `${label} → ${collectionName}` : label}
                className={cn(
                    "flex h-7 min-w-0 flex-1 items-center gap-1 rounded-md px-1.5 text-left text-sm",
                    isActive ? "bg-accent text-accent-foreground" : "hover:bg-accent/50",
                    isDimmed && "opacity-50",
                )}
            >
                <span className="truncate">{label}</span>
                {collectionName && (
                    <>
                        <ArrowRight className="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
                        <span className="truncate text-muted-foreground">{collectionName}</span>
                    </>
                )}
                {typeIcon}
                {hasError && <CircleAlert className="size-3.5 shrink-0 text-destructive" aria-label="Has errors" />}
            </button>
            <DropdownMenu>
                <DropdownMenuTrigger asChild>
                    <Button variant="ghost" size="icon" className="size-6 shrink-0" title="Table actions">
                        <EllipsisVertical className="size-3.5" aria-hidden="true" />
                    </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="min-w-[200px]">
                    {actions}
                </DropdownMenuContent>
            </DropdownMenu>
        </div>
    );
}
