import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";

import * as stories from "./DatabasesPage.stories";

const { Single, Cluster, Sharded, DifferentNodeStates, WithLoadError, WithDifferentAccessLevel } =
    composeStories(stories);

const selectors = {
    disableButton: "Disable",
    enableButton: "Enable",
    pauseIndexing: "Pause indexing until restart",
    disableIndexing: "Disable indexing",
    compactDatabase: "Compact database",
};

describe("DatabasesPage", function () {
    it("can render single view", async () => {
        const { screen } = rtlRender(<Single />);

        await screen.findByText(/Manage group/i);
        await screen.findByText("3 Indexing errors");

        expect(await screen.findAllByText(selectors.disableButton)).toHaveLength(2); // disable + disable indexing

        expect(await screen.findByText(selectors.pauseIndexing)).toBeInTheDocument();
        expect(await screen.findByText(selectors.disableIndexing)).toBeInTheDocument();
        expect(await screen.findByText(selectors.compactDatabase)).toBeInTheDocument();
    });

    it("can render cluster view", async () => {
        const { screen } = rtlRender(<Cluster />);

        await screen.findByText(/Manage group/i);

        await screen.findByText("9 Indexing errors");
    });

    it("can render load error", async () => {
        const { screen, fireClick } = rtlRender(<WithLoadError />);

        await screen.findByText(/Manage group/i);

        await screen.findByText("6 Indexing errors");
        await screen.findByText(/Database has load errors/i);
        const findAllDistributionDetailsTitle = await screen.findAllByTitle(/Expand distribution details/i);

        await fireClick(findAllDistributionDetailsTitle[0]);
    });

    it("can render sharded view", async () => {
        const { screen } = rtlRender(<Sharded />);

        await screen.findByText(/Manage group/i);

        await screen.findByText("18 Indexing errors");
    });

    it("can render node statuses", async () => {
        const { screen } = rtlRender(<DifferentNodeStates />);

        expect(await screen.findAllByText(/Manage group/i)).toHaveLength(2);

        await screen.findByText("9 Indexing errors");
        await screen.findByText("18 Indexing errors");
    });

    it("can render different access modes", async () => {
        const { screen } = rtlRender(<WithDifferentAccessLevel />);

        expect(await screen.findAllByText("9 Indexing errors")).toHaveLength(3);

        expect(screen.queryByText(/Manage group/i)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.disableButton)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.enableButton)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.disableIndexing)).not.toBeInTheDocument();
        expect(screen.queryByText(selectors.compactDatabase)).not.toBeInTheDocument();

        // db admin can pause indexing
        expect(screen.queryByText(selectors.pauseIndexing)).toBeInTheDocument();
    });
});
