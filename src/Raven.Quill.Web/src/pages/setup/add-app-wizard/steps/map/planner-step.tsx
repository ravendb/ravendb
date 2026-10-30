import { useFormContext, useWatch } from "react-hook-form";
import { AiConsentGate } from "@/components/ai-consent/ai-consent-gate";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData, AppStepId } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { isEditorMapping } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import { PlannerSchemaFigure } from "@/pages/setup/add-app-wizard/steps/map/planner-schema-figure";
import { PlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace";
import { selectHasSession } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";
import { useSwitchToManualMapping } from "@/pages/setup/add-app-wizard/steps/map/use-switch-to-manual-mapping";

export function PlannerStep({ isBusy, goToPreviousStep, goToStep }: WizardBodyComponentProps<AppStepId>) {
    const hasSession = useSetupWizardStore(selectHasSession);

    if (hasSession) {
        return <PlannerWorkspace isBusy={isBusy} onChangeSelection={goToPreviousStep} />;
    }

    return <PlannerWelcome isBusy={isBusy} onManualMapping={() => goToStep("mapTables")} />;
}

function PlannerWelcome({ isBusy, onManualMapping }: { isBusy: boolean; onManualMapping: () => void }) {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "verifySchema.tables" });
    const session = usePlannerSession();
    const switchToManualMapping = useSwitchToManualMapping();
    const setMapOrigin = useSetupWizardStore((state) => state.setMapOrigin);

    // Choosing the planner over a mapping made by hand or imported takes the editor out of the flow;
    // the plan replaces that mapping once the app is created from it.
    const start = () => {
        if (isEditorMapping(useSetupWizardStore.getState().mapOrigin)) {
            setMapOrigin(null);
        }

        void session.start();
    };

    return (
        <div className="flex min-h-80 flex-1 flex-col overflow-hidden rounded-lg border bg-background">
            <AiConsentGate
                variant="hero"
                copy={{
                    title: "The planner reads your schema, not your data",
                    description: `It suggests how your ${formatCount(tables.length, "table")} ${tables.length === 1 ? "becomes" : "become"} collections. You review every change, and nothing goes further until you move on.`,
                    acceptLabel: "Start the planner",
                }}
                figure={<PlannerSchemaFigure tables={tables} />}
                onGranted={start}
                secondaryAction={
                    <Text as="span" variant="muted">
                        or{" "}
                        <Button
                            variant="link"
                            className="h-auto p-0 text-foreground underline decoration-border underline-offset-3"
                            disabled={isBusy}
                            onClick={() => {
                                switchToManualMapping();
                                onManualMapping();
                            }}
                        >
                            use manual mapping
                        </Button>
                        .
                    </Text>
                }
            />
        </div>
    );
}
