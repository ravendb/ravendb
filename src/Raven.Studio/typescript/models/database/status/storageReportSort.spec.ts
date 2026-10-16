import storageReportItem from "models/database/status/storageReportItem";
import { storageReportSort } from "models/database/status/storageReportSort";

function reportItem(name: string, type: string, size: number, dataSize: number = null) {
    const item = new storageReportItem(name, type, true, size);
    item.dataSize = dataSize;
    return item;
}

function namesOf(items: storageReportItem[]) {
    return items.map((x) => x.name);
}

describe("storageReportSort", () => {
    it("lists the largest items first by default", () => {
        const sort = new storageReportSort();
        const items = [
            reportItem("Small", "database", 10),
            reportItem("Large", "database", 300),
            reportItem("Medium", "database", 50),
        ];

        expect(namesOf(sort.apply(items))).toEqual(["Large", "Medium", "Small"]);
    });

    it("sorts names in natural order and reverses them on a second click", () => {
        const sort = new storageReportSort();
        const items = [
            reportItem("Journal #10", "journal", 1),
            reportItem("Journal #2", "journal", 2),
            reportItem("Journal #1", "journal", 3),
        ];

        sort.sortBy("name");
        expect(namesOf(sort.apply(items))).toEqual(["Journal #1", "Journal #2", "Journal #10"]);

        sort.sortBy("name");
        expect(namesOf(sort.apply(items))).toEqual(["Journal #10", "Journal #2", "Journal #1"]);
    });

    it("sorts by type alphabetically", () => {
        const sort = new storageReportSort();
        const items = [
            reportItem("Server", "system", 1),
            reportItem("Orders", "database", 2),
            reportItem("Sharded (shard #0)", "shard", 3),
        ];

        sort.sortBy("type");

        expect(namesOf(sort.apply(items))).toEqual(["Orders", "Sharded (shard #0)", "Server"]);
    });

    it("starts a size column with the largest values", () => {
        const sort = new storageReportSort();
        const items = [
            reportItem("A", "database", 1, 100),
            reportItem("B", "database", 1, 300),
            reportItem("C", "database", 1, 200),
        ];

        sort.sortBy("name");
        sort.sortBy("dataSize");

        expect(namesOf(sort.apply(items))).toEqual(["B", "C", "A"]);
    });

    it("keeps items without a value last in both directions", () => {
        const sort = new storageReportSort();
        const items = [
            reportItem("Unknown", "database", 0, null),
            reportItem("A", "database", 1, 100),
            reportItem("B", "database", 1, 300),
        ];

        sort.sortBy("dataSize");
        expect(namesOf(sort.apply(items))).toEqual(["B", "A", "Unknown"]);

        sort.sortBy("dataSize");
        expect(namesOf(sort.apply(items))).toEqual(["A", "B", "Unknown"]);
    });
});
