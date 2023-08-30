import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { DatabaseAdmin, BelowDatabaseAdmin, LicenseRestricted } from "./DocumentRevisions.stories";
import { documentRevisionsConfigNames } from "./store/documentRevisionsSlice";

describe("DocumentRevisions", () => {
    it("can render for database admin", async () => {
        const { screen } = rtlRender(<DatabaseAdmin />);

        expect(await screen.findByText(documentRevisionsConfigNames.defaultDocument)).toBeInTheDocument();
        expect(screen.getByText(documentRevisionsConfigNames.defaultConflicts)).toBeInTheDocument();

        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render for access below database admin", async () => {
        const { screen } = rtlRender(<BelowDatabaseAdmin />);

        expect(await screen.findByText(documentRevisionsConfigNames.defaultDocument)).toBeInTheDocument();
        expect(screen.getByText(documentRevisionsConfigNames.defaultConflicts)).toBeInTheDocument();

        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });
    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        const licensingText = await screen.findByText(/Licensing/i);
        expect(licensingText).toBeInTheDocument();
    });
});
