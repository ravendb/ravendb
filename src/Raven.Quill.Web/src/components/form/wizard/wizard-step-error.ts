export class WizardStepError extends Error {
    readonly details?: string;

    constructor(message: string, details?: string) {
        super(message);
        this.name = "WizardStepError";
        this.details = details;
    }
}
