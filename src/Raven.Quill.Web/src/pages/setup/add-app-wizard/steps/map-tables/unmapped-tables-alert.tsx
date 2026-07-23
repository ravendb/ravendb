import { TriangleAlertIcon } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    collectMappedSourceTableKeys,
    getSourceTableKey,
    getSourceTableLabel,
    scaffoldTables,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";

/** Warns when tables selected in the verify step have no mapping that captures their data
 * (root or embedded) - e.g. when AI-suggested mappings skipped some of them. Being the
 * target of a linked table is not enough: links only reference documents by id. */
export function UnmappedTablesAlert() {
    const { control, getValues } = useFormContext<AppFormData>();
    const applyMapTables = useApplyMapTables();
    const mappedTables = useWatch({ control, name: "mapTables.tables" }) ?? [];
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    const mappedKeys = collectMappedSourceTableKeys(mappedTables);
    const unmappedTables = (getValues("verifySchema.tables") ?? []).filter((table) => {
        const key = getSourceTableKey(table);

        return key !== null && !mappedKeys.has(key);
    });

    if (unmappedTables.length === 0) {
        return null;
    }

    const handleMapTables = () => {
        applyMapTables([...getValues("mapTables.tables"), ...scaffoldTables(unmappedTables, discoverResult)]);
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
                Changes to {unmappedTables.map(getSourceTableLabel).join(", ")} will not be synced. Review your
                configuration to verify, map {unmappedTables.length === 1 ? "it" : "them"} back, or go back and deselect{" "}
                {unmappedTables.length === 1 ? "it" : "them"}.
            </AlertDescription>
            <AlertAction>
                <Button type="button" variant="outline" size="sm" onClick={handleMapTables}>
                    Map tables
                </Button>
            </AlertAction>
        </Alert>
    );
}
