import { FieldErrors } from "react-hook-form";
import { EditAiAgentFormData, editAiAgentYupResolver } from "./editAiAgentValidation";

async function getHistoryExpirationError(trimming: Partial<EditAiAgentFormData["trimming"]>) {
    const { errors }: { errors: FieldErrors<EditAiAgentFormData> } = await editAiAgentYupResolver(
        { trimming } as EditAiAgentFormData,
        { allParameterNames: [], allQueryNames: [], allActionNames: [] },
        { fields: {}, shouldUseNativeValidation: false }
    );

    return errors.trimming?.historyExpirationInSeconds;
}

describe("editAiAgentValidation", () => {
    it("requires a history expiration when it is set", async () => {
        const error = await getHistoryExpirationError({
            method: "Tokens",
            isEnableHistory: true,
            isSetHistoryExpiration: true,
            historyExpirationInSeconds: null,
        });

        expect(error).toBeDefined();
    });

    it("does not require a history expiration while history is turned off", async () => {
        const error = await getHistoryExpirationError({
            method: "Tokens",
            isEnableHistory: false,
            isSetHistoryExpiration: true,
            historyExpirationInSeconds: null,
        });

        expect(error).toBeUndefined();
    });

    it("does not require a history expiration while trimming is turned off", async () => {
        const error = await getHistoryExpirationError({
            method: null,
            isEnableHistory: true,
            isSetHistoryExpiration: true,
            historyExpirationInSeconds: null,
        });

        expect(error).toBeUndefined();
    });
});
