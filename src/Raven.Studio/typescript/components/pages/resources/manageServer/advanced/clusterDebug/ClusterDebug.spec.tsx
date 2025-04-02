import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./ClusterDebug.stories";
import { within } from "@testing-library/dom";

const { Default } = composeStories(stories);

const selectors = {
    summary: /Summary/,
    entries: /Log Entries/,
    viewDetails: /View details/,
    close: /Close/,
    connectionDetailsTitle: /Click to see connection details/,
    showItemPreviewTitle: /Show item preview/,
    connectionDetails: /Connection details/,
    waitForEntries: /Wait for entries/,
    installationProgressHeader: /Cluster Snapshot installation progress/,
};

describe("ClusterDebug", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<Default />);

        expect(await screen.findByText(selectors.summary)).toBeInTheDocument();
        expect(await screen.findByText(selectors.entries)).toBeInTheDocument();

        expect(await screen.findByText("UpdateLicenseLimitsCommand")).toBeInTheDocument();
    });

    it("can see snapshot installation details", async () => {
        const { screen, fireClick } = rtlRender(<Default />);

        const viewDetailsButton = await screen.findByText(selectors.viewDetails);
        await fireClick(viewDetailsButton);

        expect(await screen.findByText(selectors.installationProgressHeader)).toBeInTheDocument();

        expect(await screen.findByText(selectors.waitForEntries)).toBeInTheDocument();

        const closeButtons = await screen.findAllByRole("button", { name: selectors.close });
        await fireClick(closeButtons[0]);

        expect(screen.queryByText(selectors.installationProgressHeader)).not.toBeInTheDocument();
    });

    it("can see connection details", async () => {
        const { screen, fireClick } = rtlRender(<Default />);

        const connectionDetailsButtons = await screen.findAllByTitle(selectors.connectionDetailsTitle);
        expect(connectionDetailsButtons.length).toBePositive();

        await fireClick(connectionDetailsButtons[0]);

        const modal = await screen.findByRole("dialog");
        const closeButtons = await within(modal).findAllByRole("button", { name: selectors.close });
        await fireClick(closeButtons[0]);
    });

    it("can see item preview", async () => {
        const { screen, fireClick } = rtlRender(<Default />);

        const showPreviewButtons = await screen.findAllByTitle(selectors.showItemPreviewTitle);
        expect(showPreviewButtons.length).toBePositive();

        await fireClick(showPreviewButtons[0]);

        const modal = await screen.findByRole("dialog");

        const closeButtons = await within(modal).findAllByRole("button", { name: selectors.close });
        await fireClick(closeButtons[0]);
    });
});
