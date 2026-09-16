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

describe("FileUploadList", () => {
    it("shows the uploading file at the top of the collapsed list", () => {
        const { screen } = rtlRender(
            <FileUploadList items={items(6, "file4.txt")} onRemove={jest.fn()} onCancel={jest.fn()} />
        );

        const names = screen.getAllByTitle(/^file\d\.txt$/).map((x) => x.textContent);
        expect(names).toEqual(["file4.txt", "file5.txt", "file6.txt"]);
        expect(screen.getByRole("button", { name: /Show all 6 files/ })).toBeInTheDocument();
    });
});
