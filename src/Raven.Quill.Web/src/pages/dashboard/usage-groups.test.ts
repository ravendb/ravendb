import { describe, expect, it } from "vitest";
import type { QuillApplicationUsage } from "@/api/generated/server-api";
import { toUsageGroups } from "@/pages/dashboard/usage-groups";

function row(topologyId: string, applicationName: string, usage: number): QuillApplicationUsage {
    return {
        topologyId,
        applicationName,
        from: "2026-06-01T00:00:00Z",
        to: "2026-06-30T23:59:59Z",
        usage,
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

    it("sorts apps by usage descending", () => {
        const groups = toUsageGroups([row("t1", "zeta", 1), row("t2", "alpha", 2), row("t3", "omega", 3)]);

        expect(groups.map((g) => g.label)).toEqual(["omega", "alpha", "zeta"]);
    });

    it("breaks a usage tie between apps by label", () => {
        const groups = toUsageGroups([row("t1", "beta", 10), row("t2", "alpha", 10)]);

        expect(groups.map((g) => g.label)).toEqual(["alpha", "beta"]);
    });

    it("has no groups at all when nothing is reported", () => {
        expect(toUsageGroups([])).toEqual([]);
    });
});
