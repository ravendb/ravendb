import fileDownloader from "common/fileDownloader";

function readBytes(blob: Blob): Promise<Uint8Array> {
    return new Promise((resolve) => {
        const reader = new FileReader();
        reader.onload = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
        reader.readAsArrayBuffer(blob);
    });
}

describe("fileDownloader", () => {
    describe("downloadAsCsv", () => {
        const createObjectURL = jest.fn<string, [Blob]>(() => "blob:csv");
        let click: jest.SpyInstance;

        beforeEach(() => {
            createObjectURL.mockClear();
            URL.createObjectURL = createObjectURL;
            click = jest.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
        });

        afterEach(() => {
            click.mockRestore();
            document.querySelectorAll("a[download]").forEach((link) => link.remove());
        });

        it("starts a download of a UTF-8 CSV file that begins with a byte order mark", async () => {
            fileDownloader.downloadAsCsv("Name\r\nZoë", "people.csv");

            const blob = createObjectURL.mock.calls[0][0];
            expect(blob.type).toBe("text/csv;charset=utf-8");
            expect(Array.from(await readBytes(blob))).toEqual([
                0xef, 0xbb, 0xbf, 0x4e, 0x61, 0x6d, 0x65, 0x0d, 0x0a, 0x5a, 0x6f, 0xc3, 0xab,
            ]);
            expect(document.querySelector<HTMLAnchorElement>("a[download]").download).toBe("people.csv");
            expect(click).toHaveBeenCalledTimes(1);
        });
    });
});
