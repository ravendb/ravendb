import type { WizardFooterComponentProps } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { ExportConfigAction } from "@/pages/setup/add-app-wizard/steps/preview/export-config-action";
import { isEditorMapping } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import { usePlannerNext } from "@/pages/setup/add-app-wizard/steps/map/use-planner-next";
import { useApplyPlanner } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";

/** The plan becomes a mapping only when applied, so exporting applies it first. */
export function PlannerExportConfigAction({ isBusy }: WizardFooterComponentProps) {
    const applyPlanner = useApplyPlanner();
    const plannerNext = usePlannerNext();
    const isManualMapping = useSetupWizardStore((state) => isEditorMapping(state.mapOrigin));

    if (isManualMapping) {
        return null;
    }

    return (
        <ExportConfigAction
            isBusy={isBusy}
            hasMapping={!plannerNext.isNextDisabled}
            beforeExport={() => applyPlanner({ report: () => {} })}
        />
    );
}
