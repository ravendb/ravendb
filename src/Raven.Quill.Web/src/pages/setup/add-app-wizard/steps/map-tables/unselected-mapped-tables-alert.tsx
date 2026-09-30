import { TriangleAlertIcon } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ExpandableTableNames } from "@/pages/setup/add-app-wizard/steps/map-tables/expandable-table-names";
import {
    collectMappedSourceTables,
    getSourceTableKey,
    getSourceTableLabel,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

export function UnselectedMappedTablesAlert() {
    const { control, getValues } = useFormContext<AppFormData>();
    const mappedTables = useWatch({ control, name: "mapTables.tables" }) ?? [];

    const selectedKeys = new Set(getValues("verifySchema.tables").map(getSourceTableKey));
    const unselectedTables = collectMappedSourceTables(mappedTables).filter((table) => {
        const key = getSourceTableKey(table);

        return key !== null && !selectedKeys.has(key);
    });

    if (unselectedTables.length === 0) {
        return null;
    }

    const isSingle = unselectedTables.length === 1;

    return (
        <Alert>
            <TriangleAlertIcon className="text-amber-600 dark:text-amber-400" />
            <AlertTitle>
                {isSingle
                    ? "1 deselected table will still be synced"
                    : `${unselectedTables.length} deselected tables will still be synced`}
            </AlertTitle>
            <AlertDescription>
                <span>
                    <ExpandableTableNames labels={unselectedTables.map((table) => getSourceTableLabel(table) ?? "")} />{" "}
                    {isSingle ? "was" : "were"} not selected in Verify your schema, but the mapping still includes{" "}
                    {isSingle ? "it" : "them"}. To leave {isSingle ? "it" : "them"} out, remove{" "}
                    {isSingle ? "it" : "them"} from the mapping.
                </span>
            </AlertDescription>
        </Alert>
    );
}
