import { useEffect, useEffectEvent } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Sparkles } from "lucide-react";
import { AiConsentGate } from "@/components/ai-consent/ai-consent-gate";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Button } from "@/components/shadcn/ui/button";
import { Heading, Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { PlannerFrame } from "@/pages/setup/add-app-wizard/steps/map/planner-frame";
import { PlannerSchemaFigure } from "@/pages/setup/add-app-wizard/steps/map/planner-schema-figure";
import { PlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

export function PlannerStep({ isBusy, goToPreviousStep }: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "verifySchema.tables" });
    const hasMapping = useWatch({ control, name: "mapTables.tables", compute: (mapped) => mapped.length > 0 });
    const consent = useAiConsent();
    const hasStarted = useSetupWizardStore((state) => state.plannerTurns.length > 0);
    const session = usePlannerSession();

    // There is no first prompt to write, so the planner starts as soon as the step opens with the terms
    // accepted - unless the form already holds a mapping, which the planner must not replace unasked.
    const shouldStart = consent.isGranted && !hasStarted && !hasMapping;
    const startSession = useEffectEvent(() => void session.start());

    useEffect(() => {
        if (shouldStart) {
            startSession();
        }
    }, [shouldStart]);

    if (hasStarted) {
        return <PlannerWorkspace isBusy={isBusy} onChangeSelection={goToPreviousStep} />;
    }

    return (
        <PlannerFrame>
            <AiConsentGate
                variant="hero"
                copy={{
                    title: "The planner reads your schema, never your rows",
                    description: `It suggests how your ${formatCount(tables.length, "table")} ${tables.length === 1 ? "becomes" : "become"} collections. You review every change, and nothing reaches the mapping until you apply it.`,
                    acceptLabel: "Accept and start",
                }}
                figure={<PlannerSchemaFigure tables={tables} />}
                onGranted={() => void session.start()}
            />
            {consent.isGranted && hasMapping && (
                <PlannerStartOffer tableCount={tables.length} isBusy={isBusy} onStart={() => void session.start()} />
            )}
        </PlannerFrame>
    );
}

/** The form already holds a mapping, which Next keeps; the planner starts only when asked to. */
function PlannerStartOffer({
    tableCount,
    isBusy,
    onStart,
}: {
    tableCount: number;
    isBusy: boolean;
    onStart: () => void;
}) {
    return (
        <div className="m-auto flex max-w-[56ch] flex-col items-center gap-4 px-6 py-10 text-center">
            <div className="flex flex-col gap-2">
                <Heading as="h3" variant="subsection">
                    Next keeps the mapping you have
                </Heading>
                <Text variant="muted">
                    The planner can suggest a new one from the {formatCount(tableCount, "selected table")}. Applying its
                    plan replaces the current mapping.
                </Text>
            </div>
            <Button disabled={isBusy} onClick={onStart}>
                <Sparkles data-icon="inline-start" />
                Start the planner
            </Button>
        </div>
    );
}
