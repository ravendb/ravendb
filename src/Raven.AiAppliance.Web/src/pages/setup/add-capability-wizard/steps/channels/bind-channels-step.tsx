import { useFormContext } from "react-hook-form";
import { Button } from "@/components/shadcn/ui/button";
import { Alert } from "@/components/shadcn/ui/alert";
import { Spinner } from "@/components/shadcn/ui/spinner";
import type { WizardBodyComponentProps, WizardFooterComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentStepId } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { StepSection } from "@/pages/setup/add-app-wizard/app-wizard-step-section";

export function BindChannelsStep(props: WizardBodyComponentProps) {
    return (
        <StepSection {...props}>
            <Alert>
                Channel binding isn&apos;t available yet. Save the agent now — you&apos;ll be able to expose it through
                web widgets and bots once channel management ships.
            </Alert>
        </StepSection>
    );
}

// The final step submits the wizard form (which provisions the agent), so its footer swaps the
// generic "Submit" for "Save agent" and reflects the form's submitting state.
export function BindChannelsFooter(props: WizardFooterComponentProps<AgentStepId>) {
    const { formState } = useFormContext();
    const prevStep = props.stepPosition === "first" ? undefined : props.prevStep;

    return (
        <div className="flex border-t px-4 py-2">
            <div className="flex gap-2">
                <Button onClick={props.cancel} variant="outline">
                    Cancel
                </Button>
                {prevStep && (
                    <Button onClick={prevStep} variant="secondary" disabled={formState.isSubmitting}>
                        Back
                    </Button>
                )}
            </div>
            <Button type="submit" className="ml-auto" disabled={formState.isSubmitting}>
                {formState.isSubmitting && <Spinner />}
                Save agent
            </Button>
        </div>
    );
}
