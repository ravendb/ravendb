import React from "react";
import { RtlScreen, rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRevisions.stories";
import { composeStories } from "@storybook/react";
import { documentRevisionsConfigNames } from "./store/documentRevisionsSlice";

const { DefaultDocumentRevisions } = composeStories(stories);

const upgradeLicenseText = "Upgrade License";

describe("DocumentRevisions", () => {
    async function waitForLoad(screen: RtlScreen) {
        await screen.findByText(documentRevisionsConfigNames.defaultConflicts);
    }

    it("can render for database admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentRevisions databaseAccess="DatabaseAdmin" licenseType="Enterprise" isCloud={false} />
        );

        await waitForLoad(screen);

        expect(screen.queryByText(upgradeLicenseText)).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render for access below database admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentRevisions databaseAccess="DatabaseRead" licenseType="Enterprise" isCloud={false} />
        );

        await waitForLoad(screen);

        expect(screen.queryByText(upgradeLicenseText)).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentRevisions databaseAccess="DatabaseAdmin" licenseType="Community" isCloud={false} />
        );

        expect(await screen.findByText(upgradeLicenseText)).toBeInTheDocument();
    });
});
