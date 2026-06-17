import { FlatIssue, formatRange, summarizeIssues, toReplicaRange } from "./analyzerUtils";
import genUtils from "common/generalUtils";

type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;

function makeIssue(severity: IssueSeverity): FlatIssue {
    return {
        key: `${severity}-${Math.random()}`,
        title: "title",
        description: "description",
        recommendedAction: "",
        severity,
        category: "Server",
        scope: "node",
        nodeTags: ["A"],
    };
}

describe("summarizeIssues", () => {
    it("returns a clean summary for an empty list", () => {
        const summary = summarizeIssues([]);
        expect(summary.total).toBe(0);
        expect(summary.worst).toBe("None");
        expect(summary.counts).toEqual({ Error: 0, Warning: 0, Info: 0, None: 0 });
    });

    it("counts each severity and reports Error as the worst when an error is present", () => {
        const summary = summarizeIssues([
            makeIssue("Error"),
            makeIssue("Warning"),
            makeIssue("Warning"),
            makeIssue("Warning"),
            makeIssue("Info"),
        ]);
        expect(summary.total).toBe(5);
        expect(summary.counts.Error).toBe(1);
        expect(summary.counts.Warning).toBe(3);
        expect(summary.counts.Info).toBe(1);
        expect(summary.worst).toBe("Error");
    });

    it("reports Warning as the worst when there is no error", () => {
        expect(summarizeIssues([makeIssue("Warning"), makeIssue("Info")]).worst).toBe("Warning");
    });

    it("reports Info as the worst when only info issues exist", () => {
        expect(summarizeIssues([makeIssue("Info"), makeIssue("Info")]).worst).toBe("Info");
    });
});

describe("toReplicaRange", () => {
    it("returns the value as both bounds for a single replica", () => {
        expect(toReplicaRange([42])).toEqual({ min: 42, max: 42 });
    });

    it("returns the min and max across replicas", () => {
        expect(toReplicaRange([1000, 1250, 1010])).toEqual({ min: 1000, max: 1250 });
    });
});

describe("formatRange", () => {
    const asCount = (n: number) => n.toLocaleString();

    it("shows a single value when replicas agree", () => {
        expect(formatRange({ min: 5, max: 5 }, asCount)).toBe("5");
    });

    it("shows a min–max range when replicas diverge", () => {
        expect(formatRange({ min: 900, max: 950 }, asCount)).toBe("900–950");
    });

    it("collapses to a single value when the bounds format identically", () => {
        const rounded = (n: number) => Math.round(n).toString();
        expect(formatRange({ min: 1.1, max: 1.4 }, rounded)).toBe("1");
    });

    it("renders a byte-size range only when the formatted sizes differ", () => {
        const format = (n: number) => genUtils.formatBytesToSize(n);

        // ~1.0 GB vs ~1.25 GB differ once formatted
        expect(formatRange({ min: 1_084_000_000, max: 1_342_000_000 }, format)).toContain("–");

        // two near-identical sizes round to the same string -> single value
        expect(formatRange({ min: 1_084_000_000, max: 1_084_000_100 }, format)).not.toContain("–");
    });
});
