import React from "react";
import { rtlRender, within } from "test/rtlTestUtils";
import { DebugPackageStubs } from "test/stubs/DebugPackageStubs";
import NodeOverview from "./NodeOverview";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

function summaryWithOsName(osName: string): DebugPackageAnalysisSummary {
    const summary = DebugPackageStubs.analysisSummary();
    summary.SummaryPerNode.A.ClusterNodeInfo.OsName = osName;
    return summary;
}

function getOsTile(screen: ReturnType<typeof rtlRender>["screen"]): HTMLElement {
    return screen.getByTitle("OS").closest(".stat-tile");
}

describe("NodeOverview", () => {
    it("shows the node's OS", () => {
        const { screen } = rtlRender(<NodeOverview summary={summaryWithOsName("Ubuntu 24.04.5 LTS")} nodeTag="A" />);

        expect(within(getOsTile(screen)).getByText("Ubuntu 24.04.5 LTS")).toBeInTheDocument();
    });

    it("shows a placeholder when the package has no OS info for the node", () => {
        // the server sends no OS name when the package has no license details for the node (e.g. unlicensed node shortly after startup)
        const { screen } = rtlRender(<NodeOverview summary={summaryWithOsName(null)} nodeTag="A" />);

        expect(within(getOsTile(screen)).getByText("-")).toBeInTheDocument();
    });
});
