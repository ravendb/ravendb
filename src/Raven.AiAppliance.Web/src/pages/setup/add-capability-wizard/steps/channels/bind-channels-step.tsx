import { Alert } from "@/components/shadcn/ui/alert";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
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
