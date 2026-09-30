import type { WizardError } from "@/api/generated/server-api";

export class WizardStepError extends Error {
    readonly details?: string;

    constructor(message: string, details?: string) {
        super(message);
        this.name = "WizardStepError";
        this.details = details;
    }
}

/** Thrown by a step whose own body already renders the failure, so the wizard skips its error alert. */
export class WizardHandledError extends Error {
    constructor(cause: unknown) {
        super("The step already reported this failure.", { cause });
        this.name = "WizardHandledError";
    }
}

export function toError(value: unknown): Error {
    return value instanceof Error ? value : new Error(String(value));
}

export function toWizardStepError(errors: WizardError[] | undefined, fallbackMessage: string): WizardStepError {
    const list = errors ?? [];

    if (list.length <= 1) {
        return new WizardStepError(list[0]?.message || fallbackMessage, list[0]?.details || undefined);
    }

    // A long list rendered inline fills the step with red text, so the alert shows a one-line
    // summary and the individual messages move into its details collapsible.
    const details = list
        .map((error) => (error.details ? `${error.message}\n${error.details}` : error.message))
        .join("\n\n");

    return new WizardStepError(`${fallbackMessage} (${list.length} errors)`, details);
}
