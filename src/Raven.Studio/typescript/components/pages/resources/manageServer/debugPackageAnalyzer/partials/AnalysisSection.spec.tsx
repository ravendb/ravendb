import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { AnalysisSectionsProvider, useAnalysisSections } from "./AnalysisSectionsContext";
import AnalysisSection from "./AnalysisSection";

function EntryList(): React.ReactElement {
    const { entries } = useAnalysisSections();
    return <span data-testid="order">{entries.map((e) => `${e.id}:${e.label}`).join(",")}</span>;
}

describe("AnalysisSection", () => {
    it("registers sections with content in order", () => {
        const { screen } = rtlRender(
            <AnalysisSectionsProvider>
                <AnalysisSection id="overview" label="Cluster overview">
                    <div>content</div>
                </AnalysisSection>
                <AnalysisSection id="usage" label="Resource usage">
                    <div>content</div>
                </AnalysisSection>
                <EntryList />
            </AnalysisSectionsProvider>
        );
        expect(screen.getByTestId("order")).toHaveTextContent("overview:Cluster overview,usage:Resource usage");
    });

    it("does not register a section that renders nothing", () => {
        const { screen } = rtlRender(
            <AnalysisSectionsProvider>
                <AnalysisSection id="empty" label="Empty">
                    {null}
                </AnalysisSection>
                <AnalysisSection id="full" label="Full">
                    <div>content</div>
                </AnalysisSection>
                <EntryList />
            </AnalysisSectionsProvider>
        );
        expect(screen.getByTestId("order")).toHaveTextContent("full:Full");
        expect(screen.getByTestId("order")).not.toHaveTextContent("empty");
    });

    it("sets the dom id on the wrapper for anchoring", () => {
        rtlRender(
            <AnalysisSectionsProvider>
                <AnalysisSection id="overview" label="Cluster overview">
                    <div>content</div>
                </AnalysisSection>
            </AnalysisSectionsProvider>
        );
        expect(document.getElementById("overview")).not.toBeNull();
    });
});
