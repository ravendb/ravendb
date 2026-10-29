import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import FileUploadList, { FileUploadItem } from "components/common/FileUploadList";

function items(count: number, uploadingName?: string): FileUploadItem[] {
    return Array.from({ length: count }, (_, i) => {
        const file = new File(["x"], `file${i + 1}.txt`);
        return file.name === uploadingName
            ? { file, status: "uploading", loaded: 0, total: 1 }
            : { file, status: "selected" };
    });
}

function shownNames(container: HTMLElement) {
    return Array.from(container.querySelectorAll(".file-upload-list__row:not(.hidden)")).map(
        (row) => row.querySelector("[title]")?.textContent
    );
}

describe("FileUploadList", () => {
    it("caps the list at collapsedCount rows and reveals the rest via Show all", async () => {
        const { screen, container, fireClick } = rtlRender(
            <FileUploadList items={items(6)} onRemove={jest.fn()} onCancel={jest.fn()} />
        );

        expect(shownNames(container)).toEqual(["file1.txt", "file2.txt", "file3.txt"]);

        await fireClick(screen.getByRole("button", { name: /Show all 6 files/ }));

        expect(shownNames(container)).toHaveLength(6);
        expect(screen.getByRole("button", { name: /Show less/ })).toBeInTheDocument();
    });

    it("always starts the collapsed window at the first file, so the fade only ever hides rows below", () => {
        const { container } = rtlRender(
            <FileUploadList items={items(6, "file4.txt")} onRemove={jest.fn()} onCancel={jest.fn()} />
        );

        expect(shownNames(container)).toEqual(["file1.txt", "file2.txt", "file3.txt"]);
    });

    it("collapses as soon as a single file goes over the cap", () => {
        const { screen, container } = rtlRender(
            <FileUploadList items={items(4)} onRemove={jest.fn()} onCancel={jest.fn()} />
        );

        expect(shownNames(container)).toHaveLength(3);
        expect(screen.getByRole("button", { name: /Show all 4 files/ })).toBeInTheDocument();
    });

    it("does not show the toggle when every file fits", () => {
        const { screen, container } = rtlRender(
            <FileUploadList items={items(3)} onRemove={jest.fn()} onCancel={jest.fn()} />
        );

        expect(screen.queryByRole("button", { name: /Show all/ })).not.toBeInTheDocument();
        expect(shownNames(container)).toHaveLength(3);
    });

    it("shows the uploaded check once, on the badge rather than twice", () => {
        const uploaded: FileUploadItem[] = [{ file: new File(["x"], "clip.mp4"), status: "uploaded" }];

        const { container, screen } = rtlRender(
            <FileUploadList items={uploaded} onRemove={jest.fn()} onCancel={jest.fn()} />
        );

        expect(screen.getByText(/Uploaded/)).toBeInTheDocument();
        expect(container.querySelectorAll(".icon-check")).toHaveLength(1);
    });

    it("keeps the status icon for outcomes whose badge is not a check", () => {
        const failed: FileUploadItem[] = [{ file: new File(["x"], "clip.mp4"), status: "failed" }];

        const { container } = rtlRender(<FileUploadList items={failed} onRemove={jest.fn()} onCancel={jest.fn()} />);

        expect(container.querySelectorAll(".icon-danger")).toHaveLength(1);
    });

    it("keeps remove out of reach while the batch is uploading", () => {
        const uploading: FileUploadItem[] = [
            { file: new File(["x"], "done.txt"), status: "uploaded" },
            { file: new File(["x"], "busy.txt"), status: "uploading", loaded: 0, total: 1 },
            { file: new File(["x"], "queued.txt"), status: "selected" },
        ];

        const { screen } = rtlRender(
            <FileUploadList items={uploading} onRemove={jest.fn()} onCancel={jest.fn()} isUploading />
        );

        expect(screen.queryByRole("button", { name: /Remove queued.txt/ })).not.toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Cancel upload of busy.txt/ })).toBeInTheDocument();
    });

    it("labels each row with its file type", () => {
        const files: FileUploadItem[] = [
            { file: new File(["x"], "clip.mp4"), status: "selected" },
            { file: new File(["x"], "notes.md"), status: "selected" },
            { file: new File(["x"], "noextension"), status: "selected" },
        ];

        const { screen } = rtlRender(<FileUploadList items={files} onRemove={jest.fn()} onCancel={jest.fn()} />);

        expect(screen.getByText("MP4")).toBeInTheDocument();
        expect(screen.getByText("MD")).toBeInTheDocument();
        expect(screen.getByText("?")).toBeInTheDocument();
    });
});
