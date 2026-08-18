import { useState } from "react";
import { TriangleAlertIcon } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import {
    Dialog,
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ExpandableTableNames } from "@/pages/setup/add-app-wizard/steps/map-tables/expandable-table-names";
import {
    collectMappedSourceTableKeys,
    getSourceTableKey,
    getSourceTableLabel,
    scaffoldTables,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";

type UnmappedTable = {
    key: string;
    table: AppFormData["verifySchema"]["tables"][number];
};

/** Warns when tables selected in the verify step have no mapping that captures their data
 * (root or embedded) - e.g. when AI-suggested mappings skipped some of them. Being the
 * target of a linked table is not enough: links only reference documents by id. */
export function UnmappedTablesAlert() {
    const { control, getValues } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();
    const mappedTables = useWatch({ control, name: "mapTables.tables" }) ?? [];
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    const mappedKeys = collectMappedSourceTableKeys(mappedTables);
    const unmappedTables: UnmappedTable[] = getValues("verifySchema.tables").flatMap((table) => {
        const key = getSourceTableKey(table);

        return key !== null && !mappedKeys.has(key) ? [{ key, table }] : [];
    });

    if (unmappedTables.length === 0) {
        return null;
    }

    const handleMapTables = (tables: AppFormData["verifySchema"]["tables"]) => {
        applyMapTables([...getValues("mapTables.tables"), ...scaffoldTables(tables, discoverResult)]);
    };

    return (
        <Alert>
            <TriangleAlertIcon className="text-amber-600 dark:text-amber-400" />
            <AlertTitle>
                {unmappedTables.length === 1
                    ? "1 selected table is not mapped"
                    : `${unmappedTables.length} selected tables are not mapped`}
            </AlertTitle>
            <AlertDescription>
                <span>
                    Changes to{" "}
                    <ExpandableTableNames
                        labels={unmappedTables.map(({ table }) => getSourceTableLabel(table) ?? "")}
                    />{" "}
                    will not be synced. Review your configuration to verify, map{" "}
                    {unmappedTables.length === 1 ? "it" : "them"} back, or go back and deselect{" "}
                    {unmappedTables.length === 1 ? "it" : "them"}.
                </span>
            </AlertDescription>
            <AlertAction>
                <MapTablesDialog unmappedTables={unmappedTables} onMapTables={handleMapTables} />
            </AlertAction>
        </Alert>
    );
}

function MapTablesDialog({
    unmappedTables,
    onMapTables,
}: {
    unmappedTables: UnmappedTable[];
    onMapTables: (tables: AppFormData["verifySchema"]["tables"]) => void;
}) {
    const [isOpen, setIsOpen] = useState(false);

    const handleConfirm = (selectedKeys: Set<string>) => {
        onMapTables(unmappedTables.filter(({ key }) => selectedKeys.has(key)).map(({ table }) => table));
        setIsOpen(false);
    };

    return (
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
            <DialogTrigger asChild>
                <Button type="button" variant="outline" size="sm">
                    Map tables
                </Button>
            </DialogTrigger>
            <DialogContent>
                {/* The body carries the selection state and is remounted on every open, so a
                    reopened dialog always starts with all tables checked. */}
                <MapTablesDialogBody unmappedTables={unmappedTables} onConfirm={handleConfirm} />
            </DialogContent>
        </Dialog>
    );
}

function MapTablesDialogBody({
    unmappedTables,
    onConfirm,
}: {
    unmappedTables: UnmappedTable[];
    onConfirm: (selectedKeys: Set<string>) => void;
}) {
    const [selectedKeys, setSelectedKeys] = useState<Set<string>>(() => new Set(unmappedTables.map(({ key }) => key)));

    const toggleTable = (key: string, isChecked: boolean) => {
        setSelectedKeys((previous) => {
            const next = new Set(previous);

            if (isChecked) {
                next.add(key);
            } else {
                next.delete(key);
            }

            return next;
        });
    };

    return (
        <>
            <DialogHeader>
                <DialogTitle>Map unmapped tables</DialogTitle>
                <DialogDescription>
                    Choose the tables to map. Each one is added as a root table pre-filled from the discovered schema.
                </DialogDescription>
            </DialogHeader>
            <div className="max-h-80 space-y-1 overflow-y-auto">
                {unmappedTables.map(({ key, table }) => (
                    <label key={key} className="flex items-center gap-2 rounded-md px-1 py-1.5 text-sm hover:bg-muted">
                        <Checkbox
                            checked={selectedKeys.has(key)}
                            onCheckedChange={(value) => toggleTable(key, value === true)}
                        />
                        <span className="truncate font-mono">{getSourceTableLabel(table)}</span>
                    </label>
                ))}
            </div>
            <DialogFooter>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </DialogClose>
                <Button type="button" disabled={selectedKeys.size === 0} onClick={() => onConfirm(selectedKeys)}>
                    {selectedKeys.size === 1 ? "Map 1 table" : `Map ${selectedKeys.size} tables`}
                </Button>
            </DialogFooter>
        </>
    );
}
