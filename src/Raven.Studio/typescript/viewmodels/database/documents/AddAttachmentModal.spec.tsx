import React from "react";
import { act, rtlRender, RtlScreen, waitFor, within } from "test/rtlTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import document from "models/database/documents/document";
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");
import AddAttachmentModal from "./AddAttachmentModal";

type ProgressCallback = (progress: attachmentUploadProgress) => void;
type User = ReturnType<typeof rtlRender>["user"];

function progress(fileName: string, status: attachmentUploadStatus, loaded: number, total: number) {
    return { position: 2, count: 2, fileName, status, loaded, total };
}

function uploadAll(files: File[], _dto: unknown, onProgress: ProgressCallback) {
    files.forEach((file) => onProgress(progress(file.name, "uploaded", file.size, file.size)));
    return Promise.resolve();
}

function rowOf(screen: RtlScreen, fileName: string) {
    return within(screen.getByText(fileName).closest(".file-upload-item") as HTMLElement);
}

describe("AddAttachmentModal", () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();
    const doc = ko.observable(new document({ "@metadata": { "@id": "users/1" } } as documentDto));
    const fileA = new File(["aaaa"], "a.txt");
    const fileB = new File(["bb"], "b.txt");
    let uploadFiles: jest.SpyInstance;

    beforeEach(() => {
        uploadFiles = jest.spyOn(editDocumentUploader.prototype, "uploadFiles").mockImplementation(uploadAll);
    });

    afterEach(() => {
        uploadFiles.mockRestore();
    });

    function renderModal(onClose = jest.fn()) {
        return rtlRender(<AddAttachmentModal document={doc} db={db} onUploaded={jest.fn()} onClose={onClose} />);
    }

    async function selectFiles(screen: RtlScreen, user: User, files: File[]) {
        const fileInput = await screen.findByTestId("file-input");
        await act(() => user.upload(fileInput, files));
    }

    async function clickUpload(screen: RtlScreen, user: User) {
        const uploadButton = await screen.findByRole("button", { name: /Upload attachment/ });
        await waitFor(() => expect(uploadButton).toBeEnabled());
        await act(() => user.click(uploadButton));
    }

    it("uploads every selected file without remote parameters", async () => {
        const onClose = jest.fn();
        const { screen, user } = renderModal(onClose);

        await selectFiles(screen, user, [fileA, fileB]);
        await clickUpload(screen, user);

        await waitFor(() => expect(uploadFiles).toHaveBeenCalledTimes(1));
        const [uploadedFiles, remoteParameters] = uploadFiles.mock.calls[0];
        expect(uploadedFiles).toEqual([fileA, fileB]);
        expect(remoteParameters).toBeUndefined();
        expect(onClose).toHaveBeenCalled();
    });

    it("replaces a file selected again under the same name", async () => {
        const { screen, user } = renderModal();
        const newerFileA = new File(["aaaaaaaa"], "a.txt");

        await selectFiles(screen, user, [fileA, fileB]);
        await selectFiles(screen, user, [newerFileA]);

        expect(screen.getAllByText("a.txt")).toHaveLength(1);
        await clickUpload(screen, user);
        const [uploadedFiles] = uploadFiles.mock.calls[0];
        expect(uploadedFiles).toEqual([fileB, newerFileA]);
    });

    it("stays open when a file does not make it, so the outcome is still on screen", async () => {
        const onClose = jest.fn();
        uploadFiles.mockImplementation((_files: File[], _dto: unknown, onProgress: ProgressCallback) => {
            onProgress(progress("a.txt", "uploaded", 4, 4));
            onProgress(progress("b.txt", "failed", 0, 2));
            return Promise.resolve();
        });
        const { screen, user } = renderModal(onClose);
        await selectFiles(screen, user, [fileA, fileB]);

        await clickUpload(screen, user);

        expect(onClose).not.toHaveBeenCalled();
        expect(await rowOf(screen, "b.txt").findByText(/Failed/)).toBeInTheDocument();
    });
});
