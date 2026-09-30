import downloader from "common/downloader";
import messagePublisher from "common/messagePublisher";

describe("downloader", () => {
    const url = "/attachments?id=some-doc-id&name=testtesttest.png";
    const fetchMock = jest.fn<Promise<Response>, Parameters<typeof fetch>>();
    let reportError: jest.SpyInstance;
    let frame: HTMLIFrameElement;

    const fakeResponse = (init: { ok: boolean; status: number; statusText: string; body?: string }): Response =>
        ({
            ok: init.ok,
            status: init.status,
            statusText: init.statusText,
            text: () => Promise.resolve(init.body ?? ""),
        }) as unknown as Response;

    beforeEach(() => {
        frame = document.createElement("iframe");
        frame.id = "downloadFrame";
        document.body.appendChild(frame);

        fetchMock.mockReset();
        global.fetch = fetchMock;
        reportError = jest.spyOn(messagePublisher, "reportError").mockImplementation(() => undefined);
    });

    afterEach(() => {
        reportError.mockRestore();
        frame.remove();
    });

    it("starts the download in the hidden frame when the server accepts the request", async () => {
        fetchMock.mockResolvedValue(fakeResponse({ ok: true, status: 200, statusText: "OK" }));

        await new downloader().download("db1", url);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(fetchMock.mock.calls[0][0]).toEqual(expect.stringContaining("/databases/db1" + url));
        expect(frame.getAttribute("src")).toEqual(expect.stringContaining("/databases/db1" + url));
        expect(reportError).not.toHaveBeenCalled();
    });

    it("reports the server error instead of downloading when the request fails", async () => {
        const errorBody = JSON.stringify({
            Message: "Connection refused (127.0.0.1:9000)",
            Error: "System.Net.Http.HttpRequestException: Connection refused (127.0.0.1:9000)",
        });
        fetchMock.mockResolvedValue(
            fakeResponse({ ok: false, status: 500, statusText: "Internal Server Error", body: errorBody })
        );

        await new downloader().download("db1", url);

        expect(frame.getAttribute("src")).toBeNull();
        expect(reportError).toHaveBeenCalledWith("Failed to download file", errorBody, "Internal Server Error");
    });

    it("reports a network failure instead of downloading", async () => {
        fetchMock.mockRejectedValue(new TypeError("Failed to fetch"));

        await new downloader().download("db1", url);

        expect(frame.getAttribute("src")).toBeNull();
        expect(reportError).toHaveBeenCalledWith("Failed to download file", "Failed to fetch");
    });
});
