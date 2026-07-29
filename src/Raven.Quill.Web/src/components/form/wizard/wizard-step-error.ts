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
    const message = list.map((error) => error.message).join("\n") || fallbackMessage;
    const details =
        list
            .map((error) => error.details)
            .filter(Boolean)
            .join("\n\n") || undefined;
    return new WizardStepError(message, details);
}
