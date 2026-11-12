import databaseStorageReportItem from "models/database/status/databaseStorageReportItem";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;

describe("databaseStorageReportItem", () => {
    it("links a database to its own storage report", () => {
        const item = new databaseStorageReportItem(DatabasesStubs.studioDatabaseState("Northwind"), false);

        expect(item.name).toBe("Northwind");
        expect(item.type).toBe("database");
        expect(item.link).toBe("#databases/status/storage/report?&database=Northwind");
    });

    it("links a shard straight to that shard's storage report", () => {
        const item = new databaseStorageReportItem(DatabasesStubs.studioDatabaseState("Orders$1"), false);

        expect(item.name).toBe("Orders (shard #1)");
        expect(item.type).toBe("shard");
        expect(item.link).toBe("#databases/status/storage/report?&database=Orders%241");
    });

    it("has no link to a disabled database, since its storage report can't open", () => {
        const item = new databaseStorageReportItem(DatabasesStubs.studioDatabaseState("Archive"), true);

        expect(item.link).toBeNull();
    });

    it("counts data and temporary buffers into the database size", () => {
        const state: StudioDatabaseState = {
            ...DatabasesStubs.studioDatabaseState("Northwind"),
            TotalSize: { SizeInBytes: 1000, HumaneSize: "1000 Bytes" },
            TempBuffersSize: { SizeInBytes: 24, HumaneSize: "24 Bytes" },
        };

        const item = new databaseStorageReportItem(state, false);

        expect(item.size).toBe(1024);
        expect(item.dataSize).toBe(1000);
        expect(item.tempBuffersSize).toBe(24);
    });

    it("shows n/a for a database that failed to load", () => {
        const item = new databaseStorageReportItem(faultedState(), false);

        expect(item.size).toBe(0);
        expect(item.formatSize(false)).toBe("n/a");
        expect(item.formatSize(true)).toBe("n/a");
        expect(item.formatPercentage(1024)).toBe("n/a");
        expect(item.formatDataSize()).toBe("n/a");
        expect(item.formatTempBuffersSize()).toBe("n/a");
    });

    it("has no link to a database that failed to load and keeps its error", () => {
        const item = new databaseStorageReportItem(faultedState(), false);

        expect(item.link).toBeNull();
        expect(item.loadError).toBe("Cannot open database");
    });
});

function faultedState() {
    return {
        Name: "Broken",
        LoadError: "Cannot open database",
        DatabaseStatus: "Error",
    } as StudioDatabaseState;
}
