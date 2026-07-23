import { Alert } from "@/components/shadcn/ui/alert";
import { WizardErrorDetails } from "@/components/form/wizard/wizard-error-details";
import { WizardStepError } from "@/components/form/wizard/wizard-step-error";

export function WizardErrorAlert({ error }: { error: Error }) {
    const details = error instanceof WizardStepError ? error.details : undefined;

    return (
        <Alert variant="destructive" className="grid gap-2">
            <span className="whitespace-pre-wrap">{error.message}</span>
            {details && <WizardErrorDetails details={details} />}
        </Alert>
    );
}
