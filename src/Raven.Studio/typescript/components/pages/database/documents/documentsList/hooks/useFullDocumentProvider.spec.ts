import { MockProviders, renderHook } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { FullDocumentProvider, useFullDocumentProvider } from "./useFullDocumentProvider";

const DATABASE_NAME = "db1";

describe("useFullDocumentProvider", () => {
    const renderProvider = () =>
        renderHook(() => useFullDocumentProvider(DATABASE_NAME), { wrapper: MockProviders }).result.current;

    const previewDocuments = async (provider: FullDocumentProvider, ...ids: string[]) => {
        for (const id of ids) {
            await provider.getDocumentPreviewSource(DatabasesStubs.documentWithMetadata(id)).load();
        }
    };

    const orderIds = (from: number, to: number) =>
        Array.from({ length: to - from + 1 }, (_, index) => `orders/${from + index}-A`);

    const getDocument = () => mockServices.databasesService.getMock("getDocumentWithMetadata");

    beforeEach(() => {
        mockServices.databasesService.withGeneratedDocumentWithMetadata();
        mockServices.databasesService.withDocumentRevisionsCount();
    });

    it("loads a document again once 50 others were previewed after it", async () => {
        const provider = renderProvider();
        await previewDocuments(provider, ...orderIds(1, 51));
        getDocument().mockClear();

        await previewDocuments(provider, "orders/51-A", "orders/2-A", "orders/1-A");

        expect(getDocument()).toHaveBeenCalledTimes(1);
        expect(getDocument()).toHaveBeenCalledWith("orders/1-A", DATABASE_NAME, true);
    });

    it("keeps a document that was previewed again among the 50 most recent", async () => {
        const provider = renderProvider();
        await previewDocuments(provider, ...orderIds(1, 50), "orders/1-A", "orders/51-A");
        getDocument().mockClear();

        await previewDocuments(provider, "orders/1-A", "orders/2-A");

        expect(getDocument()).toHaveBeenCalledTimes(1);
        expect(getDocument()).toHaveBeenCalledWith("orders/2-A", DATABASE_NAME, true);
    });

    it("keeps a newer document when an older request for it fails later", async () => {
        let failFirstRequest: (error: Error) => void;
        getDocument().mockClear();
        getDocument().mockImplementationOnce(() => new Promise((_, reject) => (failFirstRequest = reject)));
        const provider = renderProvider();

        const firstPreview = previewDocuments(provider, "orders/1-A");
        provider.clearCache();
        await previewDocuments(provider, "orders/1-A");
        failFirstRequest(new Error("The server is down"));
        await expect(firstPreview).rejects.toThrow("The server is down");

        await previewDocuments(provider, "orders/1-A");

        expect(getDocument()).toHaveBeenCalledTimes(2);
    });
});
