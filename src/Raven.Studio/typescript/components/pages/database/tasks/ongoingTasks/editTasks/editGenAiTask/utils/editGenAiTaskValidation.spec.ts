import { FieldErrors } from "react-hook-form";
import { EditGenAiTaskFormData, editGenAiTaskResolver } from "./editGenAiTaskValidation";

async function getTracingExpirationError(formData: Partial<EditGenAiTaskFormData>) {
    const { errors }: { errors: FieldErrors<EditGenAiTaskFormData> } = await editGenAiTaskResolver(
        formData as EditGenAiTaskFormData,
        { allQueryNames: [] },
        { fields: {}, shouldUseNativeValidation: false }
    );

    return errors.tracingExpirationInSeconds;
}

describe("editGenAiTaskValidation", () => {
    it("requires a tracing expiration when it is set", async () => {
        const error = await getTracingExpirationError({
            isEnableTracing: true,
            isSetTracingExpiration: true,
            tracingExpirationInSeconds: null,
        });

        expect(error).toBeDefined();
    });

    it("does not require a tracing expiration while tracing is turned off", async () => {
        const error = await getTracingExpirationError({
            isEnableTracing: false,
            isSetTracingExpiration: true,
            tracingExpirationInSeconds: null,
        });

        expect(error).toBeUndefined();
    });

    it("does not require a tracing expiration while it is not set", async () => {
        const error = await getTracingExpirationError({
            isEnableTracing: true,
            isSetTracingExpiration: false,
            tracingExpirationInSeconds: null,
        });

        expect(error).toBeUndefined();
    });
});
