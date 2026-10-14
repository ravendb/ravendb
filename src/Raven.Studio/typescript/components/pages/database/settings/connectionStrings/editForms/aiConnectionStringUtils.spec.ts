import { aiConnectionStringUtils } from "./aiConnectionStringUtils";
import { AiConnection, ConnectionFormData } from "../connectionStringsTypes";

type FormData = ConnectionFormData<AiConnection>;

describe("aiConnectionStringUtils", () => {
    describe("getConnectorOptions", () => {
        const optionValues = (modelType: FormData["modelType"]) =>
            aiConnectionStringUtils.getConnectorOptions(modelType).map((x) => x.value);

        it("offers Anthropic for chat models", () => {
            expect(optionValues("Chat")).toContain("anthropicSettings");
        });

        it("hides Anthropic for text embeddings because it has no embeddings API", () => {
            expect(optionValues("TextEmbeddings")).not.toContain("anthropicSettings");
            expect(optionValues("TextEmbeddings")).toContain("openAiSettings");
        });
    });
});
