import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import FileUploadSummary from "components/common/FileUploadSummary";
import { FileUploadItem, FileUploadItemStatus } from "components/common/FileUploadList";

function item(name: string, size: number, status: FileUploadItemStatus, loaded?: number): FileUploadItem {
    const file = new File([new Uint8Array(size)], name);
    return { file, status, loaded, total: size };
}

describe("FileUploadSummary", () => {
    it("totals the selection and clears it on demand", async () => {
        const onClearAll = jest.fn();
        const { screen, fireClick } = rtlRender(
            <FileUploadSummary
                items={[item("a.txt", 1024, "selected"), item("b.txt", 1024, "selected")]}
                onClearAll={onClearAll}
            />
        );

        expect(screen.getByText("2 files • 2 KB")).toBeInTheDocument();

        await fireClick(screen.getByRole("button", { name: /Clear all/ }));
        expect(onClearAll).toHaveBeenCalled();
    });

    it("counts a finished file towards the batch position while one is still uploading", () => {
        const { screen } = rtlRender(
            <FileUploadSummary
                items={[
                    item("a.txt", 1024, "uploaded"),
                    item("b.txt", 1024, "uploading", 512),
                    item("c.txt", 1024, "selected"),
                ]}
                onClearAll={jest.fn()}
            />
        );

        expect(screen.getByText("Uploading 2 of 3")).toBeInTheDocument();
    });

    it("rolls the outcome up once every file has settled", () => {
        const { screen } = rtlRender(
            <FileUploadSummary
                items={[
                    item("a.txt", 1024, "uploaded"),
                    item("b.txt", 1024, "uploaded"),
                    item("c.txt", 1024, "skipped"),
                ]}
                onClearAll={jest.fn()}
            />
        );

        expect(screen.getByText("2 uploaded")).toBeInTheDocument();
        expect(screen.getByText("1 skipped")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Clear all/ })).not.toBeInTheDocument();
    });
});
