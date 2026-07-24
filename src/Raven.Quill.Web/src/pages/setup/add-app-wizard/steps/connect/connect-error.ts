import type { WizardError } from "@/api/generated/server-api";
import { WizardStepError } from "@/components/form/wizard/wizard-step-error";

export function toConnectionError(errors: WizardError[] | undefined): WizardStepError {
    const list = errors ?? [];
    const message = list.map((error) => error.message).join("\n") || "Connection failed.";
    const details =
        list
            .map((error) => error.details)
            .filter(Boolean)
            .join("\n\n") || undefined;
    return new WizardStepError(message, details);
}
