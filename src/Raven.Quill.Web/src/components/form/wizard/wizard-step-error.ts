import type { WizardError } from "@/api/generated/server-api";

export class WizardStepError extends Error {
    readonly details?: string;

    constructor(message: string, details?: string) {
        super(message);
        this.name = "WizardStepError";
        this.details = details;
    }
}

export function toWizardStepError(errors: WizardError[] | undefined, fallbackMessage: string): WizardStepError {
    const list = errors ?? [];
    const message = list.map((error) => error.message).join("\n") || fallbackMessage;
    const details =
        list
            .map((error) => error.details)
            .filter(Boolean)
            .join("\n\n") || undefined;
    return new WizardStepError(message, details);
}
