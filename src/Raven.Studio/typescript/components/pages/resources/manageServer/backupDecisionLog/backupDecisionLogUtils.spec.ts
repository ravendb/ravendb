import { BackupDecisionLogEntry, emptyBackupDecisionFilters } from "./backupDecisionLogTypes";
import { getDetailLabel, mergeDecisionLogEntries, toDecisionLogQuery } from "./backupDecisionLogUtils";

const entry = (
    Id: number,
    LastTime: string,
    overrides: Partial<BackupDecisionLogEntry> = {}
): BackupDecisionLogEntry => ({
    Id,
    FirstTime: LastTime,
    LastTime,
    Count: 1,
    Category: "Waiting",
    Detail: "BackupTime",
    Reason: "Not due yet",
    Source: "Task",
    Database: "Orders",
    Shard: null,
    TaskId: 1,
    TaskName: "Daily",
    NodeTag: "A",
    ...overrides,
});

describe("backupDecisionLogUtils", () => {
    describe("mergeDecisionLogEntries", () => {
        it("replaces an entry with the same id instead of adding a duplicate", () => {
            const current = [entry(2, "2026-11-09T10:01:00Z"), entry(1, "2026-11-09T10:00:00Z")];
            const changed = [entry(1, "2026-11-09T10:05:00Z", { Count: 6 })];

            const merged = mergeDecisionLogEntries(current, changed, 10);

            expect(merged.map((x) => [x.Id, x.Count])).toEqual([
                [1, 6],
                [2, 1],
            ]);
        });

        it("sorts newest first, breaks ties by id and keeps only the newest take entries", () => {
            const current = [entry(1, "2026-11-09T10:00:00Z"), entry(2, "2026-11-09T10:00:00Z")];
            const changed = [entry(3, "2026-11-09T09:00:00Z")];

            const merged = mergeDecisionLogEntries(current, changed, 2);

            expect(merged.map((x) => x.Id)).toEqual([2, 1]);
        });
    });

    it("toDecisionLogQuery repeats list filters and sends dates as ISO", () => {
        const query = toDecisionLogQuery(
            {
                ...emptyBackupDecisionFilters,
                databases: ["Orders", "Invoices"],
                taskIds: ["7"],
                events: ["Failed", "HeldBack"],
                reason: "disk full",
                dateFrom: new Date("2026-11-09T10:00:00Z"),
            },
            512
        );

        expect(query).toBe(
            "take=512&database=Orders&database=Invoices&taskId=7&category=Failed&category=HeldBack&reason=disk+full&from=2026-11-09T10%3A00%3A00.000Z"
        );
    });

    it("toDecisionLogQuery sends only take without filters", () => {
        expect(toDecisionLogQuery(emptyBackupDecisionFilters, 512)).toBe("take=512");
    });

    it("getDetailLabel maps known details and returns unknown ones as they are", () => {
        expect(getDetailLabel("CpuCredits")).toBe("CPU credits");
        expect(getDetailLabel("task-deleted")).toBe("Task deleted");
        expect(getDetailLabel("SomethingNew")).toBe("SomethingNew");
        expect(getDetailLabel(null)).toBeNull();
    });
});
