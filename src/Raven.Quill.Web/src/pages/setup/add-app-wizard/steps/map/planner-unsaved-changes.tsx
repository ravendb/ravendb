import { Pencil } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { mapDtoTableToForm } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { PlanProblems, PlanWarning } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-collection-parts";
import { usePlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import { WHOLE_PLAN_ISSUES } from "@/pages/setup/add-app-wizard/steps/map/session/planner-edits";
import {
    selectIsAnyChatWorking,
    selectUnsavedCount,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

export function PlannerUnsavedChanges() {
    const { isBusy } = usePlannerWorkspace();
    const { session, openCollection } = usePlannerActions();
    const unsavedCount = useSetupWizardStore(selectUnsavedCount);
    const isWorking = useSetupWizardStore(selectIsAnyChatWorking);
    const isSaving = useSetupWizardStore((state) => state.plannerIsSaving);
    const planIssues = useSetupWizardStore((state) => state.plannerSaveIssues[WHOLE_PLAN_ISSUES]);
    const warnings = useSetupWizardStore((state) => state.plannerSaveWarnings);
    const discardPlannerEdits = useSetupWizardStore((state) => state.discardPlannerEdits);
    const dismissPlannerSaveWarnings = useSetupWizardStore((state) => state.dismissPlannerSaveWarnings);

    // The editor checks a mapping's structure as it is typed; a collection left with errors is opened
    // again instead of being sent for the server to refuse.
    const save = () => {
        const edits = useSetupWizardStore.getState().plannerEdits;
        const invalid = Object.entries(edits).find(
            ([, edit]) => edit.config && !tablesSchema.safeParse([mapDtoTableToForm(edit.config)]).success,
        );

        if (invalid) {
            const [collection, edit] = invalid;

            openCollection(edit.area, collection);
            toast.error(`Fix the mapping of ${collection} before saving.`);
            return;
        }

        void session.save();
    };

    if (unsavedCount === 0) {
        return warnings.length > 0 ? (
            <PlanWarning>
                <div className="flex flex-col gap-1">
                    {warnings.map((warning, index) => (
                        <span key={index}>{warning}</span>
                    ))}
                    <Button variant="link" className="h-auto self-start p-0" onClick={dismissPlannerSaveWarnings}>
                        Dismiss
                    </Button>
                </div>
            </PlanWarning>
        ) : null;
    }

    return (
        <div role="status" className="flex flex-col gap-3 rounded-lg border border-primary/50 bg-primary/8 px-4 py-3">
            <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">
                <div className="flex min-w-0 flex-[1_1_15rem] flex-col">
                    <Text as="span" variant="label">
                        You have {formatCount(unsavedCount, "unsaved change")}
                    </Text>
                    <Text as="span" variant="muted">
                        {isWorking
                            ? "Save once the conversation is done; the planner is working on the plan."
                            : "Nothing reaches the plan until you save."}
                    </Text>
                </div>
                <div className="flex gap-2">
                    <Button variant="outline" disabled={isSaving} onClick={discardPlannerEdits}>
                        Discard
                    </Button>
                    <Button disabled={isSaving || isWorking || isBusy} onClick={save}>
                        {isSaving && <Spinner />}
                        Save plan
                    </Button>
                </div>
            </div>
            {planIssues && <PlanProblems problems={planIssues.map((issue) => issue.message)} />}
        </div>
    );
}

export function EditsDisabledNote() {
    const isWorking = useSetupWizardStore(selectIsAnyChatWorking);

    if (!isWorking) {
        return null;
    }

    return (
        <Text variant="caption" className="flex items-center gap-1.5">
            <Pencil className="size-3" aria-hidden="true" />
            Manual edits are off while a conversation is working.
        </Text>
    );
}
