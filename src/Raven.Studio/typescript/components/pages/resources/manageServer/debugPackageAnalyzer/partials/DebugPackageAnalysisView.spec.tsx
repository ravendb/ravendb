import React from "react";
import { rtlRender, waitFor } from "test/rtlTestUtils";
import DebugPackageAnalysisView from "./DebugPackageAnalysisView";
import { DebugPackageStubs } from "test/stubs/DebugPackageStubs";
import { flattenIssues } from "./analyzerUtils";

describe("DebugPackageAnalysisView rail", () => {
    it("lists cluster sections in the rail by default", () => {
        const { screen } = rtlRender(<DebugPackageAnalysisView summary={DebugPackageStubs.analysisSummary()} />);
        expect(screen.getByRole("button", { name: "Analysis Results" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Cluster Overview" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Resource Usage" })).toBeInTheDocument();
    });

    it("swaps the section list when the scope changes to Node", async () => {
        const { screen, fireClick } = rtlRender(
            <DebugPackageAnalysisView summary={DebugPackageStubs.analysisSummary()} />
        );
        await fireClick(screen.getByRole("tab", { name: /Node\b/i }));
        expect(await screen.findByRole("button", { name: "Node Overview" })).toBeInTheDocument();
        await waitFor(() => expect(screen.queryByRole("button", { name: "Cluster Overview" })).not.toBeInTheDocument());
    });

    it("lists database sections when the scope changes to Database", async () => {
        const { screen, fireClick } = rtlRender(
            <DebugPackageAnalysisView summary={DebugPackageStubs.analysisSummary()} />
        );
        await fireClick(screen.getByRole("tab", { name: /Database\b/i }));
        expect(await screen.findByRole("button", { name: "Database Overview" })).toBeInTheDocument();
    });

    // The scope selectors belong in the nav rail next to the scope tabs. The database dropdown's long
    // option labels used to overflow the narrow rail; menuPosition="fixed" lets the open menu escape
    // the rail's overflow instead, so the selectors can stay in the rail without a horizontal scrollbar.
    it("renders the scope selectors inside the nav rail", async () => {
        const { screen, fireClick, container } = rtlRender(
            <DebugPackageAnalysisView summary={DebugPackageStubs.analysisSummary()} />
        );
        await fireClick(screen.getByRole("tab", { name: /Database\b/i }));

        const label = await screen.findByText("Select database");
        const rail = container.querySelector(".analysis-rail");
        expect(rail).toBeInTheDocument();
        expect(rail).toContainElement(label);
        expect(container.querySelector(".analysis-content-toolbar")).not.toBeInTheDocument();
    });

    // The per-selection issue count is misleading on the generic scope tab, so it lives on the
    // node/database selector instead. Cluster keeps its (genuinely cluster-wide) tab count.
    it("shows the selected node's issue count on the node selector, not on the Node scope tab", async () => {
        const summary = DebugPackageStubs.analysisSummary();
        const { screen, fireClick, container } = rtlRender(<DebugPackageAnalysisView summary={summary} />);
        await fireClick(screen.getByRole("tab", { name: /Node\b/i }));
        await screen.findByText("Select node");

        const expected = flattenIssues(summary).filter((i) => i.nodeTags.includes("A")).length;
        expect(container.querySelector(".react-select__single-value .analysis-scope-count")).toHaveTextContent(
            String(expected)
        );
        expect(screen.getByRole("tab", { name: /Node\b/i }).querySelector(".badge")).not.toBeInTheDocument();
    });

    it("shows each node's issue count on its dropdown option", async () => {
        const summary = DebugPackageStubs.analysisSummary();
        const nodeCount = Object.keys(summary.SummaryPerNode ?? {}).length;
        const { screen, fireClick, user, container } = rtlRender(<DebugPackageAnalysisView summary={summary} />);
        await fireClick(screen.getByRole("tab", { name: /Node\b/i }));
        await screen.findByText("Select node");

        await user.click(container.querySelector(".react-select__control"));
        await screen.findByText("Node B"); // an option only rendered once the menu is open

        // one count badge per node option in the open menu
        expect(container.querySelectorAll(".react-select__menu .analysis-scope-count")).toHaveLength(nodeCount);
    });

    it("shows the selected database's issue count on the database selector, not on the Database scope tab", async () => {
        const summary = DebugPackageStubs.analysisSummary();
        const { screen, fireClick, container } = rtlRender(<DebugPackageAnalysisView summary={summary} />);
        await fireClick(screen.getByRole("tab", { name: /Database\b/i }));
        await screen.findByText("Select database");

        // databaseNames are sorted, so "Customers" is selected by default
        const expected = flattenIssues(summary).filter(
            (i) => i.scope === "database" && i.database === "Customers"
        ).length;
        expect(container.querySelector(".react-select__single-value .analysis-scope-count")).toHaveTextContent(
            String(expected)
        );
        expect(screen.getByRole("tab", { name: /Database\b/i }).querySelector(".badge")).not.toBeInTheDocument();
    });
});
