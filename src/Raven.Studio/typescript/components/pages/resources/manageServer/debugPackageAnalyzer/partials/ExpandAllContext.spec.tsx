import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { ExpandAllProvider, useExpandAll, useExpandAllSync } from "./ExpandAllContext";
import ExpandAllToggle from "./ExpandAllToggle";

// Stands in for a node-grouped table: it consumes the same sync hook the real tables use and renders
// its resulting TanStack expansion state so the test can assert on it without rendering a virtual table.
function SyncProbe({ testId }: { testId: string }) {
    const [expanded] = useExpandAllSync();
    return <span data-testid={testId}>{JSON.stringify(expanded)}</span>;
}

function Harness() {
    return (
        <ExpandAllProvider>
            <ExpandAllToggle />
            <SyncProbe testId="tableA" />
            <SyncProbe testId="tableB" />
        </ExpandAllProvider>
    );
}

const switchName = /expand all node rows/i;

describe("ExpandAll", () => {
    it("starts collapsed: the switch is off and every table's expansion state is empty", () => {
        const { screen } = rtlRender(<Harness />);
        expect(screen.getByRole("checkbox", { name: switchName })).not.toBeChecked();
        expect(screen.getByTestId("tableA")).toHaveTextContent("{}");
        expect(screen.getByTestId("tableB")).toHaveTextContent("{}");
    });

    it("expands every table at once when switched on", async () => {
        const { screen, fireClick } = rtlRender(<Harness />);
        await fireClick(screen.getByRole("checkbox", { name: switchName }));
        expect(screen.getByRole("checkbox", { name: switchName })).toBeChecked();
        expect(screen.getByTestId("tableA")).toHaveTextContent("true");
        expect(screen.getByTestId("tableB")).toHaveTextContent("true");
    });

    it("collapses every table again when switched off", async () => {
        const { screen, fireClick } = rtlRender(<Harness />);
        await fireClick(screen.getByRole("checkbox", { name: switchName }));
        await fireClick(screen.getByRole("checkbox", { name: switchName }));
        expect(screen.getByRole("checkbox", { name: switchName })).not.toBeChecked();
        expect(screen.getByTestId("tableA")).toHaveTextContent("{}");
        expect(screen.getByTestId("tableB")).toHaveTextContent("{}");
    });

    it("throws when the hook is used outside the provider", () => {
        function Outside(): null {
            useExpandAll();
            return null;
        }
        const spy = jest.spyOn(console, "error").mockImplementation(() => {});
        expect(() => rtlRender(<Outside />)).toThrow(/ExpandAllProvider/);
        spy.mockRestore();
    });
});
