import {
    BackupDecisionEvent,
    BackupDecisionLogEntry,
    BackupDecisionRow,
    emptyBackupDecisionFilters,
} from "./backupDecisionLogTypes";
import {
    filterDecisions,
    getNewestRowPerStream,
    getSubReasonLabel,
    mergeDecisionLogEntries,
    toDecisionRows,
} from "./backupDecisionLogUtils";

function logEntry(overrides: Partial<BackupDecisionLogEntry> = {}): BackupDecisionLogEntry {
    return {
        Time: "2026-11-05T10:00:00Z",
        Source: "Task",
        Database: "Orders",
        TaskId: 1,
        TaskName: "Daily",
        Kind: "Policy",
        Detail: "BackupTime",
        Reason: "Not yet time",
        ...overrides,
    };
}

function row(overrides: Partial<BackupDecisionRow> = {}): BackupDecisionRow {
    return {
        Id: "1",
        Event: "Waiting",
        SubReason: null,
        Reason: null,
        DatabaseName: "Orders",
        ShardNumber: null,
        TaskName: "Daily",
        TaskId: 1,
        FirstAt: "2026-11-05T10:00:00Z",
        LastAt: "2026-11-05T10:00:00Z",
        Count: 1,
        ...overrides,
    };
}

describe("backupDecisionLogUtils", () => {
    describe("toDecisionRows", () => {
        it("maps Policy to Waiting or HeldBack by Detail", () => {
            const details = ["Startup", "BackupTime", "BackupRunning", "DatabaseLoaded", "CpuCredits", "SomethingNew"];

            const rows = toDecisionRows(details.map((Detail) => logEntry({ Detail })));

            const expectedEvents: BackupDecisionEvent[] = [
                "Waiting",
                "Waiting",
                "Waiting",
                "Waiting",
                "HeldBack",
                "HeldBack",
            ];
            expect(rows.map((x) => x.Event)).toEqual(expectedEvents);
            expect(rows.map((x) => x.SubReason)).toEqual(details);
        });

        it("maps other kinds one to one", () => {
            const rows = toDecisionRows([
                logEntry({ Kind: "Info", Detail: null }),
                logEntry({ Kind: "Started", Detail: null }),
                logEntry({ Kind: "Completed", Detail: null }),
                logEntry({ Kind: "Failed", Detail: null }),
                logEntry({ Kind: "Cancelled", Detail: "Task deleted" }),
            ]);

            const expectedEvents: BackupDecisionEvent[] = ["Info", "Started", "Completed", "Failed", "Cancelled"];
            expect(rows.map((x) => x.Event)).toEqual(expectedEvents);
            expect(rows.at(-1).SubReason).toBe("Task deleted");
        });

        it("splits a shard database into name and shard number", () => {
            const [sharded, plain] = toDecisionRows([logEntry({ Database: "Orders$2" }), logEntry({ TaskId: 2 })]);

            expect([sharded.DatabaseName, sharded.ShardNumber]).toEqual(["Orders", 2]);
            expect([plain.DatabaseName, plain.ShardNumber]).toEqual(["Orders", null]);
        });

        it("maps server entries to server-wide rows without task", () => {
            const [serverRow] = toDecisionRows([
                logEntry({ Source: "Server", Database: null, TaskId: 0, TaskName: null, Detail: "CpuCredits" }),
            ]);

            expect(serverRow).toMatchObject({ DatabaseName: null, ShardNumber: null, TaskName: null, TaskId: null });
        });

        it("collapses a consecutive run into one row", () => {
            const rows = toDecisionRows([
                logEntry({ Time: "2026-11-05T10:02:00Z" }),
                logEntry({ Time: "2026-11-05T10:01:00Z" }),
                logEntry({ Time: "2026-11-05T10:00:00Z" }),
            ]);

            expect(rows).toEqual([
                row({
                    Id: "Task-Orders-1-Policy-BackupTime-2026-11-05T10:00:00Z",
                    SubReason: "BackupTime",
                    Reason: "Not yet time",
                    FirstAt: "2026-11-05T10:00:00Z",
                    LastAt: "2026-11-05T10:02:00Z",
                    Count: 3,
                }),
            ]);
        });

        it("collapses runs of interleaved streams and sorts rows by LastAt", () => {
            const tick = (Time: string) => [
                logEntry({ Time }),
                logEntry({ Time, TaskId: 2, Detail: "BackupDisabled", Reason: "Disabled" }),
                logEntry({ Time, Source: "Server", Database: null, TaskId: null, TaskName: null, Kind: "Info" }),
            ];

            const rows = toDecisionRows([
                ...tick("2026-11-05T10:02:00Z"),
                ...tick("2026-11-05T10:01:00Z"),
                logEntry({ Time: "2026-11-05T10:00:30Z", TaskId: 2, Kind: "Failed", Detail: null }),
                ...tick("2026-11-05T10:00:00Z"),
            ]);

            expect(rows.map((x) => [x.TaskId, x.Event, x.Count, x.LastAt])).toEqual([
                [1, "Waiting", 3, "2026-11-05T10:02:00Z"],
                [2, "HeldBack", 2, "2026-11-05T10:02:00Z"],
                [null, "Info", 3, "2026-11-05T10:02:00Z"],
                [2, "Failed", 1, "2026-11-05T10:00:30Z"],
                [2, "HeldBack", 1, "2026-11-05T10:00:00Z"],
            ]);
            expect(new Set(rows.map((x) => x.Id)).size).toBe(rows.length);
        });

        it("does not collapse identical entries separated by another entry of the same stream", () => {
            const rows = toDecisionRows([
                logEntry({ Time: "2026-11-05T10:02:00Z" }),
                logEntry({ Time: "2026-11-05T10:01:00Z", Kind: "Started", Detail: null }),
                logEntry({ Time: "2026-11-05T10:00:00Z" }),
            ]);

            expect(rows.map((x) => x.Count)).toEqual([1, 1, 1]);
            expect(new Set(rows.map((x) => x.Id)).size).toBe(3);
        });
    });

    it("getNewestRowPerStream keeps the first row of each task and the server stream", () => {
        const rows = [
            row({ Id: "a1", TaskId: 1 }),
            row({ Id: "server", DatabaseName: null, TaskName: null, TaskId: null }),
            row({ Id: "a2", TaskId: 1 }),
            row({ Id: "shard", TaskId: 1, ShardNumber: 0 }),
        ];

        expect(getNewestRowPerStream(rows).map((x) => x.Id)).toEqual(["a1", "server", "shard"]);
    });

    it("mergeDecisionLogEntries dedupes repeated server entries and sorts newest first", () => {
        const server = logEntry({
            Source: "Server",
            Database: null,
            TaskId: null,
            TaskName: null,
            Detail: "CpuCredits",
        });
        const orders = logEntry({ Time: "2026-11-05T09:00:00Z" });
        const invoices = logEntry({ Time: "2026-11-05T11:00:00Z", Database: "Invoices" });

        const result = mergeDecisionLogEntries([
            [server, orders],
            [invoices, server],
        ]);

        expect(result).toEqual([invoices, server, orders]);
    });

    it("filterDecisions matches reason text and sub-reason label case-insensitively", () => {
        const bySubReason = row({ Id: "1", SubReason: "CpuCredits", Reason: "unrelated" });
        const byReason = row({ Id: "2", Reason: "Low on CPU credits" });
        const other = row({ Id: "3", Reason: "nothing" });

        const result = filterDecisions([bySubReason, byReason, other], {
            ...emptyBackupDecisionFilters,
            reason: "cpu CREDITS",
        });

        expect(result.map((x) => x.Id)).toEqual(["1", "2"]);
    });

    it("filterDecisions filters by events and tasks", () => {
        const rows = [
            row({ Id: "1", Event: "Failed", TaskName: "Daily" }),
            row({ Id: "2", Event: "Failed", TaskName: "Hourly" }),
            row({ Id: "3", Event: "Started", TaskName: "Daily" }),
        ];

        const result = filterDecisions(rows, {
            ...emptyBackupDecisionFilters,
            events: ["Failed"],
            tasks: ["Daily"],
        });

        expect(result.map((x) => x.Id)).toEqual(["1"]);
    });

    it("filterDecisions filters by date range on LastAt", () => {
        const rows = [
            row({ Id: "before", LastAt: "2026-11-04T23:59:00Z" }),
            row({ Id: "inside", FirstAt: "2026-11-04T23:00:00Z", LastAt: "2026-11-05T10:00:00Z" }),
            row({ Id: "after", LastAt: "2026-11-06T00:01:00Z" }),
        ];

        const result = filterDecisions(rows, {
            ...emptyBackupDecisionFilters,
            dateFrom: new Date("2026-11-05T00:00:00Z"),
            dateTo: new Date("2026-11-05T23:59:59Z"),
        });

        expect(result.map((x) => x.Id)).toEqual(["inside"]);
    });

    it("getSubReasonLabel returns raw value for unknown code", () => {
        expect(getSubReasonLabel("The backup task was deleted")).toBe("The backup task was deleted");
        expect(getSubReasonLabel(null)).toBeNull();
        expect(getSubReasonLabel("CpuCredits")).toBe("CPU credits");
        expect(getSubReasonLabel("ConcurrentBackup")).toBe("Concurrent backup");
    });
});
