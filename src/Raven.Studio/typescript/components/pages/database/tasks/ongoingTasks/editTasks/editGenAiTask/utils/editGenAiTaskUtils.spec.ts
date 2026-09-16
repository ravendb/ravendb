import { editGenAiTaskUtils } from "./editGenAiTaskUtils";

const { getTestDocumentPayload } = editGenAiTaskUtils;

describe("editGenAiTaskUtils", () => {
    describe("getTestDocumentPayload", () => {
        const documentJson = JSON.stringify({
            Name: "Post",
            "@metadata": { "@collection": "Posts" },
        });

        it("should send only the document id for an existing, unedited document", () => {
            const payload = getTestDocumentPayload(documentJson, "posts/1", false);

            // Only the id is sent so the server loads the full document (with attachment
            // metadata and the HasAttachments flag) from storage, instead of overwriting it
            // with the playground body that has '@attachments' stripped.
            expect(payload.DocumentId).toBe("posts/1");
            expect(payload.Document).toBeNull();
        });

        it("should send the document body when the content was edited in the playground", () => {
            const payload = getTestDocumentPayload(documentJson, "posts/1", true);

            expect(payload.DocumentId).toBe("posts/1");
            expect(payload.Document).toEqual(JSON.parse(documentJson));
        });

        it("should send only the document body for a manually entered document (no id)", () => {
            const payload = getTestDocumentPayload(documentJson, null, true);

            expect(payload.DocumentId).toBeUndefined();
            expect(payload.Document).toEqual(JSON.parse(documentJson));
        });

        it("should send nothing when there is no playground document", () => {
            const payload = getTestDocumentPayload("", "posts/1", false);

            expect(payload.DocumentId).toBeUndefined();
            expect(payload.Document).toBeNull();
        });
    });
});
