import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRevisions.stories";
import { composeStories } from "@storybook/testing-react";
import { documentRevisionsConfigNames } from "./store/documentRevisionsSlice";

const { DefaultDocumentRevisions } = composeStories(stories);

const upgradeLicenseText = "Upgrade License";

type Screen = ReturnType<typeof rtlRender>["screen"];

describe("DocumentRevisions", () => {
    async function waitForLoad(screen: Screen) {
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
