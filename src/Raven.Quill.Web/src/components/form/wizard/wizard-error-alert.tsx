import { Alert } from "@/components/shadcn/ui/alert";
import { WizardErrorDetails } from "@/components/form/wizard/wizard-error-details";
import { WizardStepError } from "@/components/form/wizard/wizard-step-error";
import { cn } from "@/lib/utils";

export function WizardErrorAlert({ error, className }: { error: Error; className?: string }) {
    const details = error instanceof WizardStepError ? error.details : undefined;

    return (
        <Alert variant="destructive" className={cn("grid gap-2", className)}>
            <span className="whitespace-pre-wrap">{error.message}</span>
            {details && <WizardErrorDetails details={details} />}
        </Alert>
    );
}
