import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRevisions.stories";
import { documentRevisionsConfigNames } from "./store/documentRevisionsSlice";
import { composeStories } from "@storybook/testing-react";

const { DefaultDocumentRevisions } = composeStories(stories);

describe("DocumentRevisions", () => {
    it("can render for database admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentRevisions databaseAccess="DatabaseAdmin" licenseType="Enterprise" isCloud={false} />
        );

        expect(await screen.findByText(documentRevisionsConfigNames.defaultDocument)).toBeInTheDocument();
        expect(screen.getByText(documentRevisionsConfigNames.defaultConflicts)).toBeInTheDocument();

        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render for access below database admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentRevisions databaseAccess="DatabaseRead" licenseType="Enterprise" isCloud={false} />
        );

        expect(await screen.findByText(documentRevisionsConfigNames.defaultDocument)).toBeInTheDocument();
        expect(screen.getByText(documentRevisionsConfigNames.defaultConflicts)).toBeInTheDocument();

        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentRevisions databaseAccess="DatabaseAdmin" licenseType="Community" isCloud={false} />
        );

        const licensingText = await screen.findByText(/Licensing/i);
        expect(licensingText).toBeInTheDocument();
    });
});
