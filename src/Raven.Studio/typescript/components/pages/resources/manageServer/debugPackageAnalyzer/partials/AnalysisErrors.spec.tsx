import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import AnalysisErrors from "./AnalysisErrors";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

function summaryWithAnalyzeError(): DebugPackageAnalysisSummary {
    return {
        SummaryPerNode: {
            A: {
                AnalyzeErrors: {
                    Errors: [
                        {
                            ComponentName: "GcAnalyzer",
                            ErrorMessage: "Failed to parse gc.log",
                            Exception:
                                "System.IO.InvalidDataException: Unexpected end of stream\n" +
                                "   at Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.GcAnalyzer.Analyze()",
                            Severity: "Warning",
                        },
                    ],
                },
            },
        },
    } as unknown as DebugPackageAnalysisSummary;
}

describe("AnalysisErrors", () => {
    it("renders nothing when there are no analyze errors", () => {
        const { screen } = rtlRender(
            <AnalysisErrors summary={{ SummaryPerNode: {} } as unknown as DebugPackageAnalysisSummary} />
        );

        expect(screen.queryByText(/could not process part of the package/i)).not.toBeInTheDocument();
    });

    it("opens the failing component's exception in a sheet", async () => {
        const { screen, fireClick } = rtlRender(<AnalysisErrors summary={summaryWithAnalyzeError()} />);

        await fireClick(screen.getByRole("button", { name: /Show details/i }));
        await fireClick(screen.getByRole("button", { name: /View exception/i }));

        // the sheet header combines the failing component and its node
        expect(screen.getByText("GcAnalyzer on Node A")).toBeInTheDocument();

        // line wrapping is offered and enabled by default
        const wrapToggle = screen.getByRole("button", { name: /Wrap/i });
        expect(wrapToggle).toHaveAttribute("aria-pressed", "true");

        await fireClick(wrapToggle);
        expect(wrapToggle).toHaveAttribute("aria-pressed", "false");
    });
});
