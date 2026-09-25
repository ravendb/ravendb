import React from "react";
import { act, fireEvent, rtlRender } from "test/rtlTestUtils";
import FileDropzone from "./FileDropzone";

const selectors = {
    fileInput: "file-input",
    dropArea: "drop-area",
    unsupportedFileType: /files are supported/,
};

async function dropFiles(dropArea: HTMLElement, files: File[]) {
    await act(async () => {
        fireEvent.drop(dropArea, { dataTransfer: { files } });
    });
}

describe("FileDropzone", () => {
    describe("without validExtensions", () => {
        it("accepts any file selected via input", async () => {
            const onChange = jest.fn();
            const { screen, user } = rtlRender(<FileDropzone onChange={onChange} maxFiles={1} />);

            const file = new File(["hello"], "image.png", { type: "image/png" });
            await user.upload(screen.getByTestId(selectors.fileInput), file);

            expect(screen.queryByText(selectors.unsupportedFileType)).not.toBeInTheDocument();
            expect(screen.getByText(/image\.png/)).toBeInTheDocument();
            expect(onChange).toHaveBeenCalledWith([file]);
        });

        it("accepts any file dropped into drop area", async () => {
            const onChange = jest.fn();
            const { screen } = rtlRender(<FileDropzone onChange={onChange} maxFiles={1} />);

            const file = new File(["hello"], "image.png", { type: "image/png" });
            await dropFiles(screen.getByClassName(selectors.dropArea), [file]);

            expect(screen.queryByText(selectors.unsupportedFileType)).not.toBeInTheDocument();
            expect(screen.getByText(/image\.png/)).toBeInTheDocument();
            expect(onChange).toHaveBeenCalledWith([file]);
        });

        it("accepts a file without extension", async () => {
            const onChange = jest.fn();
            const { screen } = rtlRender(<FileDropzone onChange={onChange} maxFiles={1} />);

            const file = new File(["hello"], "README", { type: "text/plain" });
            await dropFiles(screen.getByClassName(selectors.dropArea), [file]);

            expect(screen.queryByText(selectors.unsupportedFileType)).not.toBeInTheDocument();
            expect(onChange).toHaveBeenCalledWith([file]);
        });
    });

    describe("drag highlight", () => {
        const dropzone = (screen: ReturnType<typeof rtlRender>["screen"]) => screen.getByClassName("file-dropzone");

        it("holds the highlight until the last nested dragleave, not the first", async () => {
            const { screen } = rtlRender(<FileDropzone onChange={jest.fn()} />);
            const dropArea = screen.getByClassName(selectors.dropArea);

            await act(async () => {
                fireEvent.dragEnter(dropArea);
                fireEvent.dragEnter(dropArea);
            });
            expect(dropzone(screen)).toHaveClass("isDragging");

            await act(async () => fireEvent.dragLeave(dropArea));
            expect(dropzone(screen)).toHaveClass("isDragging");

            await act(async () => fireEvent.dragLeave(dropArea));
            expect(dropzone(screen)).not.toHaveClass("isDragging");
        });

        it("clears the highlight on drop from any drag depth, with no dragleave to balance it", async () => {
            const { screen } = rtlRender(<FileDropzone onChange={jest.fn()} />);
            const dropArea = screen.getByClassName(selectors.dropArea);

            await act(async () => {
                fireEvent.dragEnter(dropArea);
                fireEvent.dragEnter(dropArea);
                fireEvent.dragEnter(dropArea);
            });
            expect(dropzone(screen)).toHaveClass("isDragging");

            await dropFiles(dropArea, [new File(["x"], "image.png")]);

            expect(dropzone(screen)).not.toHaveClass("isDragging");
        });

        it("drops the browse affordance from the prompt while dragging", async () => {
            const { screen } = rtlRender(<FileDropzone onChange={jest.fn()} />);
            const dropArea = screen.getByClassName(selectors.dropArea);

            expect(screen.getByText(/click to browse/)).toBeInTheDocument();

            await act(async () => fireEvent.dragEnter(dropArea));

            expect(screen.getByText(/Drop files here/)).toBeInTheDocument();
            expect(screen.queryByText(/click to browse/)).not.toBeInTheDocument();

            await act(async () => fireEvent.dragLeave(dropArea));

            expect(screen.getByText(/click to browse/)).toBeInTheDocument();
        });

        it("says a file, not files, when only one is accepted", async () => {
            const { screen } = rtlRender(<FileDropzone onChange={jest.fn()} maxFiles={1} />);

            await act(async () => fireEvent.dragEnter(screen.getByClassName(selectors.dropArea)));

            expect(screen.getByText(/Drop a file here/)).toBeInTheDocument();
            expect(screen.queryByText(/click to browse/)).not.toBeInTheDocument();
        });

        it("pulses on a drop that carried files", async () => {
            const { screen } = rtlRender(<FileDropzone onChange={jest.fn()} />);
            const dropArea = screen.getByClassName(selectors.dropArea);

            await dropFiles(dropArea, [new File(["x"], "image.png")]);
            expect(dropzone(screen)).toHaveClass("isDropped");
        });

        it("does not pulse when nothing was actually dropped", async () => {
            const { screen } = rtlRender(<FileDropzone onChange={jest.fn()} />);
            const dropArea = screen.getByClassName(selectors.dropArea);

            await dropFiles(dropArea, []);
            expect(dropzone(screen)).not.toHaveClass("isDropped");
        });
    });

    describe("with validExtensions", () => {
        it("rejects a dropped file with extension outside the list", async () => {
            const onChange = jest.fn();
            const { screen } = rtlRender(<FileDropzone onChange={onChange} maxFiles={1} validExtensions={["zip"]} />);

            const file = new File(["hello"], "image.png", { type: "image/png" });
            await dropFiles(screen.getByClassName(selectors.dropArea), [file]);

            expect(screen.getByText(selectors.unsupportedFileType)).toBeInTheDocument();
            expect(onChange).toHaveBeenCalledWith([]);
        });

        it("accepts a file with extension inside the list", async () => {
            const onChange = jest.fn();
            const { screen, user } = rtlRender(
                <FileDropzone onChange={onChange} maxFiles={1} validExtensions={["zip"]} />
            );

            const file = new File(["hello"], "package.zip", { type: "application/zip" });
            await user.upload(screen.getByTestId(selectors.fileInput), file);

            expect(screen.queryByText(selectors.unsupportedFileType)).not.toBeInTheDocument();
            expect(onChange).toHaveBeenCalledWith([file]);
        });
    });
});
