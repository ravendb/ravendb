import bucketReportItem = require("models/database/status/bucketReportItem");

function createItem(fromRange: number, size: number, numberOfBuckets: number, documentsCount: number): bucketReportItem {
    const item = new bucketReportItem(String(fromRange), size, numberOfBuckets, documentsCount, []);
    item.fromRange = fromRange;
    item.toRange = fromRange;
    return item;
}

describe("bucketReportItem.nextSort", () => {
    it("starts a numeric column descending and the range column ascending", () => {
        expect(bucketReportItem.nextSort(null, "size")).toEqual({ column: "size", direction: "desc" });
        expect(bucketReportItem.nextSort(null, "range")).toEqual({ column: "range", direction: "asc" });
    });

    it("clears the sorting on the third click of the same column", () => {
        const firstClick = bucketReportItem.nextSort(null, "documents");
        const secondClick = bucketReportItem.nextSort(firstClick, "documents");
        const thirdClick = bucketReportItem.nextSort(secondClick, "documents");

        expect(firstClick).toEqual({ column: "documents", direction: "desc" });
        expect(secondClick).toEqual({ column: "documents", direction: "asc" });
        expect(thirdClick).toBeNull();
    });

    it("restarts the cycle when a different column is clicked", () => {
        const current: bucketReportSort = { column: "documents", direction: "asc" };

        expect(bucketReportItem.nextSort(current, "buckets")).toEqual({ column: "buckets", direction: "desc" });
    });
});

describe("bucketReportItem.sortComparator", () => {
    const first = createItem(300, 100, 3, 50);
    const second = createItem(100, 300, 1, 10);
    const third = createItem(200, 200, 2, 90);

    const sorted = (column: bucketReportSortColumn, direction: bucketReportSortDirection) =>
        [first, second, third].sort(bucketReportItem.sortComparator(column, direction)).map(x => x.fromRange);

    it("sorts by range", () => {
        expect(sorted("range", "asc")).toEqual([100, 200, 300]);
        expect(sorted("range", "desc")).toEqual([300, 200, 100]);
    });

    it("sorts by non-empty buckets count", () => {
        expect(sorted("buckets", "asc")).toEqual([100, 200, 300]);
        expect(sorted("buckets", "desc")).toEqual([300, 200, 100]);
    });

    it("sorts by documents count", () => {
        expect(sorted("documents", "asc")).toEqual([100, 300, 200]);
        expect(sorted("documents", "desc")).toEqual([200, 300, 100]);
    });

    it("sorts by size", () => {
        expect(sorted("size", "asc")).toEqual([300, 200, 100]);
        expect(sorted("size", "desc")).toEqual([100, 200, 300]);
    });

    it("breaks ties by range regardless of direction", () => {
        const items = [createItem(500, 10, 1, 1), createItem(100, 10, 1, 1), createItem(300, 10, 1, 1)];

        expect(items.slice().sort(bucketReportItem.sortComparator("size", "desc")).map(x => x.fromRange))
            .toEqual([100, 300, 500]);
        expect(items.slice().sort(bucketReportItem.sortComparator("size", "asc")).map(x => x.fromRange))
            .toEqual([100, 300, 500]);
    });
});
