import { buildStorageRows } from "./StoragePerDatabase";
import { buildDatabasesOverviewRows } from "./DatabasesOverview";
import { DebugPackageStubs } from "test/stubs/DebugPackageStubs";

const mb = 1024 * 1024;

// The stub replicates every database on nodes A/B/C with slightly different per-node values, which is
// exactly the case the rollup must not sum. Orders storage is 512/498/505 MB; documents are 12,345 on
// every node.
describe("buildStorageRows", () => {
    const rows = buildStorageRows(DebugPackageStubs.analysisSummary());
    const orders = rows.find((r) => r.database === "Orders")!;

    it("shows the per-node spread on the parent row, never the sum of replicas", () => {
        // a sum would be ~1515 MB; the rollup must stay within a single replica's range
        expect(orders.size).toEqual({ min: 498 * mb, max: 512 * mb });
    });

    it("computes Total from per-node totals, not range-of-size plus range-of-temp", () => {
        // per-node totals: 512+32, 498+30, 505+31 -> min 528, max 544 MB
        expect(orders.total).toEqual({ min: 528 * mb, max: 544 * mb });
    });

    it("carries a single value (min === max) on each per-node sub-row", () => {
        const nodeA = orders.subRows!.find((r) => r.nodeTag === "A")!;
        expect(nodeA.size).toEqual({ min: 512 * mb, max: 512 * mb });
    });
});

describe("buildDatabasesOverviewRows", () => {
    const rows = buildDatabasesOverviewRows(DebugPackageStubs.analysisSummary());
    const orders = rows.find((r) => r.database === "Orders")!;

    it("collapses identical replica counts to a single value (min === max)", () => {
        // every node reports 12,345 documents -> the parent agrees, no spread
        expect(orders.documentsCount).toEqual({ min: 12345, max: 12345 });
    });

    it("keeps per-node counts on the sub-rows so the spread is one expand away", () => {
        const nodeB = orders.subRows!.find((r) => r.nodeTag === "B")!;
        expect(nodeB.documentsCount).toEqual({ min: 12345, max: 12345 });
    });
});
