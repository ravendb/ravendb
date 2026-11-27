import { getDocumentPreviewLines } from "components/common/virtualTable/utils/documentPreviewLines";
import { act, fireEvent, rtlRender } from "test/rtlTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { CellWithDocumentPreview, DocumentPreviewSource } from "./CellWithDocumentPreview";

jest.mock("components/common/virtualTable/utils/documentPreviewLines", () => {
    const actual = jest.requireActual("components/common/virtualTable/utils/documentPreviewLines");
    return { ...actual, getDocumentPreviewLines: jest.fn(actual.getDocumentPreviewLines) };
});

describe("CellWithDocumentPreview", () => {
    const previewSource: DocumentPreviewSource = {
        load: async () => ({ document: DatabasesStubs.documentWithMetadata("orders/1-A"), revisionsCount: 29 }),
    };

    const renderCell = () => (
        <CellWithDocumentPreview id="orders/1-A" previewSource={previewSource}>
            <span>orders/1-A</span>
        </CellWithDocumentPreview>
    );

    beforeEach(() => {
        jest.useFakeTimers();
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it("formats the document once while the card stays open", async () => {
        const getLines = jest.mocked(getDocumentPreviewLines);
        const { screen, rerender } = rtlRender(renderCell());

        fireEvent.mouseEnter(screen.getByText("orders/1-A").closest(".cell-preview-target"));
        await act(async () => {
            jest.advanceTimersByTime(1000);
        });
        expect(screen.getByRole("tooltip")).toHaveTextContent('"Company"');
        getLines.mockClear();

        await act(async () => {
            rerender(renderCell());
        });

        expect(screen.getByRole("tooltip")).toHaveTextContent('"Company"');
        expect(getLines).not.toHaveBeenCalled();
    });
});
