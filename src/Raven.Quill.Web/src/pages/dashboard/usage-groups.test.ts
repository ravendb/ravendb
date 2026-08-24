import { describe, expect, it } from "vitest";
import type { QuillApplicationUsage } from "@/api/generated/server-api";
import { SYSTEM_GROUP_KEY, SYSTEM_GROUP_LABEL, toUsageGroups } from "@/pages/dashboard/usage-groups";

function row(topologyId: string, applicationName: string, usage: number, isSystem = false): QuillApplicationUsage {
    return {
        topologyId,
        applicationName,
        from: "2026-06-01T00:00:00Z",
        to: "2026-06-30T23:59:59Z",
        usage,
        isSystem,
    };
}

describe("toUsageGroups", () => {
    it("keeps a uniquely named app as a plain, unexpandable row", () => {
        const [group] = toUsageGroups([row("t1", "support-copilot", 5200)]);

        expect(group).toMatchObject({ label: "support-copilot", isExpandable: false, usage: 5200 });
        expect(group!.rows).toHaveLength(1);
    });

    it("collapses same-named apps into one counted group", () => {
        // Two appliances under one licence can both run an app called "huetopia"; as separate rows
        // they read as duplicates, so they collapse into a group the count and ids explain.
        const groups = toUsageGroups([row("t1", "huetopia", 300), row("t2", "huetopia", 700)]);

        const group = groups.find((g) => g.label === "huetopia")!;
        expect(group.isExpandable).toBe(true);
        expect(group.usage).toBe(1000);
        expect(group.rows.map((r) => r.topologyId)).toEqual(["t2", "t1"]); // heaviest first
    });

    it("collapses every system row into one group, whatever the config database is called", () => {
        const groups = toUsageGroups([
            row("t1", "quill-config", 40, true),
            row("t2", "quill-config", 12, true),
            row("t3", "acme-config", 900, true),
        ]);

        const group = groups.find((g) => g.key === SYSTEM_GROUP_KEY)!;
        expect(group.label).toBe(SYSTEM_GROUP_LABEL);
        expect(group.rows).toHaveLength(3);
        expect(group.usage).toBe(952);
    });

    it("expands a lone system row, unlike a lone app", () => {
        // The system label hides which database it is, so expanding has to stay the way back to that
        // even for the single-appliance case.
        const [group] = toUsageGroups([row("t1", "quill-config", 40, true)]);

        expect(group!.isExpandable).toBe(true);
    });

    it("sorts the system group last and keeps apps in the order they arrived", () => {
        const groups = toUsageGroups([
            row("t9", "quill-config", 40, true),
            row("t1", "zeta", 1),
            row("t2", "alpha", 2),
        ]);

        expect(groups.map((g) => g.label)).toEqual(["zeta", "alpha", SYSTEM_GROUP_LABEL]);
    });

    it("never groups an app with a system row that shares its name", () => {
        // "quill-config" is only the default; an app can legitimately be called that on an appliance
        // configured with a different config database, and must not be folded into System.
        const groups = toUsageGroups([row("t1", "quill-config", 500), row("t2", "quill-config", 40, true)]);

        expect(groups).toHaveLength(2);
        expect(groups.find((g) => g.key === SYSTEM_GROUP_KEY)!.usage).toBe(40);
        expect(groups.find((g) => g.label === "quill-config" && !g.isSystem)!.usage).toBe(500);
    });

    it("has no groups at all when nothing is reported", () => {
        expect(toUsageGroups([])).toEqual([]);
    });
});
