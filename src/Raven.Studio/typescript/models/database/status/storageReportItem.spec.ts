import storageReportItem from "models/database/status/storageReportItem";

describe("storageReportItem", () => {
    it("formats the data and temporary buffers sizes", () => {
        const item = new storageReportItem("Northwind", "documents", true, 3584);
        item.dataSize = 2048;
        item.tempBuffersSize = 1536;

        expect(item.formatDataSize()).toBe("2 KB");
        expect(item.formatTempBuffersSize()).toBe("1.5 KB");
    });

    it("shows n/a for data and temporary buffers sizes it doesn't have", () => {
        const item = new storageReportItem("Datafile", "data", false, 1024);

        expect(item.formatDataSize()).toBe("n/a");
        expect(item.formatTempBuffersSize()).toBe("n/a");
    });
});
