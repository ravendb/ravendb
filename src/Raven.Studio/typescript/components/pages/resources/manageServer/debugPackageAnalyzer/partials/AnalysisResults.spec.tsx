import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import AnalysisResults from "./AnalysisResults";
import { FlatIssue } from "./analyzerUtils";

type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;

function makeIssue(severity: IssueSeverity, title: string): FlatIssue {
    return {
        key: title,
        title,
        description: "description",
        recommendedAction: "",
        severity,
        category: "Server",
        scope: "node",
        nodeTags: ["A"],
    };
}

describe("AnalysisResults", () => {
    it("shows issues from every severity at once by default", () => {
        const { screen } = rtlRender(
            <AnalysisResults
                issues={[
                    makeIssue("Error", "An error issue"),
                    makeIssue("Warning", "A warning issue"),
                    makeIssue("Info", "An info issue"),
                ]}
            />
        );

        expect(screen.getByText("An error issue")).toBeInTheDocument();
        expect(screen.getByText("A warning issue")).toBeInTheDocument();
        expect(screen.getByText("An info issue")).toBeInTheDocument();
    });

    it("collapses a group's rows while keeping its header", async () => {
        const { screen, fireClick } = rtlRender(<AnalysisResults issues={[makeIssue("Error", "An error issue")]} />);

        await fireClick(screen.getByRole("button", { name: /^Errors/i }));

        expect(screen.queryByText("An error issue")).not.toBeInTheDocument();
        expect(screen.getByRole("button", { name: /^Errors/i })).toHaveAttribute("aria-expanded", "false");
    });

    it("caps a large group, peeks one extra row, and reveals the rest via Show all", async () => {
        const warnings = Array.from({ length: 7 }, (_, i) => makeIssue("Warning", `Warning ${i + 1}`));
        const { screen, fireClick } = rtlRender(<AnalysisResults issues={warnings} />);

        // The 6th row peeks out from under the fade gradient; the 7th stays hidden until "Show all".
        expect(screen.getByText("Warning 6")).toBeInTheDocument();
        expect(screen.queryByText("Warning 7")).not.toBeInTheDocument();

        await fireClick(screen.getByRole("button", { name: /Show all 7 Warnings/i }));

        expect(screen.getByText("Warning 7")).toBeInTheDocument();

        // "Show less" collapses back to the capped view.
        await fireClick(screen.getByRole("button", { name: /Show less/i }));

        expect(screen.queryByText("Warning 7")).not.toBeInTheDocument();
    });

    it("shows the empty state when there are no issues", () => {
        const { screen } = rtlRender(<AnalysisResults issues={[]} />);

        expect(screen.getByText(/No analysis results match the selected filters/i)).toBeInTheDocument();
    });
});
