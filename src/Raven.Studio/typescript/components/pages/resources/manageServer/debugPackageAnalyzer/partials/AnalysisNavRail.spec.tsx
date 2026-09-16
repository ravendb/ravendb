import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import AnalysisNavRail, { ScopeItem } from "./AnalysisNavRail";
import { AnalysisSectionEntry } from "./AnalysisSectionsContext";

const scopeItems: ScopeItem<"cluster" | "node" | "database">[] = [
    { value: "cluster", label: "Cluster", icon: "cluster", count: 3 },
    { value: "node", label: "Node", icon: "node", count: 1 },
    { value: "database", label: "Database", icon: "database", count: 2 },
];

const sections: AnalysisSectionEntry[] = [
    { id: "results", label: "Analysis results" },
    { id: "overview", label: "Cluster overview" },
];

describe("AnalysisNavRail", () => {
    it("renders scope rows with counts and marks the selected one", () => {
        const { screen } = rtlRender(
            <AnalysisNavRail
                scopeItems={scopeItems}
                selectedScope="cluster"
                onSelectScope={() => undefined}
                sections={sections}
                activeSectionId="results"
                onSelectSection={() => undefined}
            />
        );
        expect(screen.getByRole("tab", { name: /Cluster\b.*3/i })).toHaveClass("active");
        expect(screen.getByRole("tab", { name: /Node\b.*1/i })).toBeInTheDocument();
    });

    it("fires onSelectScope when a scope row is clicked", async () => {
        const onSelectScope = jest.fn();
        const { screen, fireClick } = rtlRender(
            <AnalysisNavRail
                scopeItems={scopeItems}
                selectedScope="cluster"
                onSelectScope={onSelectScope}
                sections={sections}
                activeSectionId="results"
                onSelectSection={() => undefined}
            />
        );
        await fireClick(screen.getByRole("tab", { name: /Database\b.*2/i }));
        expect(onSelectScope).toHaveBeenCalledWith("database");
    });

    it("renders the section list and marks the active item, firing onSelectSection on click", async () => {
        const onSelectSection = jest.fn();
        const { screen, fireClick } = rtlRender(
            <AnalysisNavRail
                scopeItems={scopeItems}
                selectedScope="cluster"
                onSelectScope={() => undefined}
                sections={sections}
                activeSectionId="results"
                onSelectSection={onSelectSection}
            />
        );
        const activeItem = screen.getByRole("button", { name: "Analysis results" });
        expect(activeItem).toHaveClass("active");
        await fireClick(screen.getByRole("button", { name: "Cluster overview" }));
        expect(onSelectSection).toHaveBeenCalledWith("overview");
    });
});
