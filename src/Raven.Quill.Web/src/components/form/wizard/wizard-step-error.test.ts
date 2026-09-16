import { toWizardStepError } from "@/components/form/wizard/wizard-step-error";
import { describe, expect, it } from "vitest";

describe("toWizardStepError", () => {
    it("uses the fallback message when there are no errors", () => {
        const error = toWizardStepError([], "Something failed.");

        expect(error.message).toBe("Something failed.");
        expect(error.details).toBeUndefined();
    });

    it("keeps a single error inline", () => {
        const error = toWizardStepError([{ message: "Table is missing.", details: "stack" }], "Something failed.");

        expect(error.message).toBe("Table is missing.");
        expect(error.details).toBe("stack");
    });

    it("summarizes multiple errors and moves the messages into details", () => {
        const error = toWizardStepError(
            [{ message: "First failed." }, { message: "Second failed.", details: "stack" }],
            "Something failed.",
        );

        expect(error.message).toBe("Something failed. (2 errors)");
        expect(error.details).toBe("First failed.\n\nSecond failed.\nstack");
    });
});
