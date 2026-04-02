import { chatAiAgentAttachmentsUtils } from "./chatAiAgentAttachmentsUtils";

const { createReservedNames, createUniqueName, getLocalFilesFromClipboardData, prepareLocalFiles } =
    chatAiAgentAttachmentsUtils;

describe("chatAiAgentAttachmentsUtils", () => {
    describe("createReservedNames", () => {
        it("should normalize all reserved names to lowercase", () => {
            const reservedNames = createReservedNames(["File.txt", "Image.PNG"]);

            expect(Array.from(reservedNames)).toEqual(["file.txt", "image.png"]);
        });
    });

    describe("createUniqueName", () => {
        it("should return the original name if it's not in reserved names", () => {
            const reservedNames = new Set(["file1.txt", "file2.txt"]);
            const name = "file3.txt";

            expect(createUniqueName(name, reservedNames)).toBe(name);
        });

        it("should append numbered suffix before file extension", () => {
            const reservedNames = new Set(["file.txt"]);

            expect(createUniqueName("file.txt", reservedNames)).toBe("file(1).txt");
        });

        it("should increment existing numbered suffix", () => {
            const reservedNames = new Set(["file(1).txt"]);

            expect(createUniqueName("file(1).txt", reservedNames)).toBe("file(2).txt");
        });

        it("should append numbered suffix for names without extension", () => {
            const reservedNames = new Set(["file"]);

            expect(createUniqueName("file", reservedNames)).toBe("file(1)");
        });

        it("should skip already reserved numbered variants", () => {
            const reservedNames = new Set(["file.txt", "file(1).txt", "file(2).txt"]);

            expect(createUniqueName("file.txt", reservedNames)).toBe("file(3).txt");
        });

        it("should treat reserved names as case insensitive", () => {
            const reservedNames = new Set(["file.txt", "file(1).txt"]);

            expect(createUniqueName("FILE.txt", reservedNames)).toBe("FILE(2).txt");
        });

        it("should honor reserved names combined from pending and persisted attachments", () => {
            const reservedNames = createReservedNames(["screen1.png", "screen1(1).png", "screen2.png"]);

            expect(createUniqueName("screen1.png", reservedNames)).toBe("screen1(2).png");
        });
    });

    describe("getLocalFilesFromClipboardData", () => {
        it("should return clipboard files when available", () => {
            const file = new File(["content"], "note.txt", { type: "text/plain" });
            const clipboardData = {
                files: [file],
                items: [
                    {
                        kind: "file",
                        getAsFile: () => new File(["other"], "ignored.txt", { type: "text/plain" }),
                    },
                ],
            } as unknown as DataTransfer;

            expect(getLocalFilesFromClipboardData(clipboardData)).toEqual([file]);
        });

        it("should fall back to clipboard items for screenshots", () => {
            const screenshot = new File(["image"], "", { type: "image/png" });
            const clipboardData = {
                files: [],
                items: [
                    {
                        kind: "file",
                        getAsFile: () => screenshot,
                    },
                ],
            } as unknown as DataTransfer;

            expect(getLocalFilesFromClipboardData(clipboardData)).toEqual([screenshot]);
        });
    });

    describe("prepareLocalFiles", () => {
        const fixedDate = new Date(2026, 2, 23, 12, 34, 56);
        let dateNowSpy: jest.SpiedFunction<typeof Date.now>;

        beforeEach(() => {
            dateNowSpy = jest.spyOn(Date, "now").mockReturnValue(fixedDate.getTime());
        });

        afterEach(() => {
            dateNowSpy.mockRestore();
        });

        it("should assign a fallback name to pasted screenshots", () => {
            const screenshot = new File(["image"], "", { type: "image/png" });

            const { attachments, invalidFiles } = prepareLocalFiles([screenshot], []);

            expect(invalidFiles).toEqual([]);
            expect(attachments).toHaveLength(1);
            expect(attachments[0].name).toBe("pasted-image-2026-03-23_12-34-56.png");
        });

        it("should deduplicate fallback clipboard names against existing attachments", () => {
            const screenshot = new File(["image"], "", { type: "image/png" });
            const { attachments: firstAttachments } = prepareLocalFiles([screenshot], []);
            const firstAttachmentName = firstAttachments[0].name;

            const { attachments } = prepareLocalFiles([screenshot], [firstAttachmentName]);

            expect(attachments).toHaveLength(1);
            expect(firstAttachmentName).toBe("pasted-image-2026-03-23_12-34-56.png");
            expect(attachments[0].name).toBe("pasted-image-2026-03-23_12-34-56(1).png");
        });
    });
});
