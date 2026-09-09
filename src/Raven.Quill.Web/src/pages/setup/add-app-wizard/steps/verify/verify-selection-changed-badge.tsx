import { Badge } from "@/components/shadcn/ui/badge";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getSourceTableKey } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

type SelectedTables = AppFormData["verifySchema"]["tables"];

export function VerifySelectionChangedBadge({ tables }: { tables: SelectedTables }) {
    const initialSelectedTables = useSetupWizardStore((state) => state.initialSelectedTables);

    if (initialSelectedTables === null || haveSameTables(tables, initialSelectedTables)) {
        return null;
    }

    return <Badge variant="warning">Selection changed</Badge>;
}

function haveSameTables(current: SelectedTables, initial: SelectedTables): boolean {
    if (current.length !== initial.length) {
        return false;
    }

    const currentKeys = new Set(current.map(getSourceTableKey));

    return initial.every((table) => currentKeys.has(getSourceTableKey(table)));
}
