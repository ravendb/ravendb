import { FieldErrors } from "react-hook-form";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "./chatAiAgentValidation";

async function getExpirationError(formData: Partial<ChatAiAgentFormData>, isDocumentExpirationEnabled = false) {
    const { errors }: { errors: FieldErrors<ChatAiAgentFormData> } = await chatAiAgentYupResolver(
        formData as ChatAiAgentFormData,
        { areParametersRequired: false, isDocumentExpirationEnabled },
        { fields: {}, shouldUseNativeValidation: false }
    );

    return errors.persistenceExpiresInSeconds;
}

describe("chatAiAgentValidation", () => {
    it("requires an expiration when a custom expiration is set and expiration is turned on in the chat", async () => {
        const error = await getExpirationError({
            isEnableDocumentExpiration: true,
            isDocumentExpireInCustomizeEnabled: true,
            persistenceExpiresInSeconds: null,
        });

        expect(error).toBeDefined();
    });

    it("requires an expiration when a custom expiration is set and expiration is turned on in the database", async () => {
        const error = await getExpirationError(
            {
                isEnableDocumentExpiration: false,
                isDocumentExpireInCustomizeEnabled: true,
                persistenceExpiresInSeconds: null,
            },
            true
        );

        expect(error).toBeDefined();
    });

    it("does not require an expiration while a custom expiration is not set", async () => {
        const error = await getExpirationError({
            isEnableDocumentExpiration: true,
            isDocumentExpireInCustomizeEnabled: false,
            persistenceExpiresInSeconds: null,
        });

        expect(error).toBeUndefined();
    });

    it("does not require an expiration while expiration is turned off", async () => {
        const error = await getExpirationError({
            isEnableDocumentExpiration: false,
            isDocumentExpireInCustomizeEnabled: true,
            persistenceExpiresInSeconds: null,
        });

        expect(error).toBeUndefined();
    });
});
