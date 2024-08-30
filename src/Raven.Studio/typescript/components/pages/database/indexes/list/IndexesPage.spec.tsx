import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";

import * as stories from "./IndexesPage.stories";

const {
    EmptyView,
    SampleDataCluster,
    FaultyIndexSharded,
    FaultyIndexSingleNode,
    LicenseLimits: CommunityLimits,
} = composeStories(stories);

describe("IndexesPage", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        await screen.findByText(/No indexes have been created for this database/i);
    });

    it("can render", async () => {
        const { screen } = rtlRender(<SampleDataCluster />);

        await screen.findByText("Orders/ByCompany");
        await screen.findByText("ReplacementOf/Orders/ByCompany");
        const deleteButtons = await screen.findAllByTitle(/Delete the index/i);
        expect(deleteButtons.length).toBeGreaterThanOrEqual(1);
    });

    it("can show search engine - corax", async () => {
        const { screen, getQueriesForElement } = rtlRender(<SampleDataCluster />);

        const orderTotals = await screen.findByText("Orders/ByCompany");
        const indexItem = orderTotals.closest(".rich-panel-item");
        const indexItemSelectors = getQueriesForElement(indexItem);

        expect(await indexItemSelectors.findByText(/Corax/)).toBeInTheDocument();
    });

    it("can open faulty index - sharded", async () => {
        const { screen } = rtlRender(<FaultyIndexSharded />);

        const openFaultyButtons = await screen.findAllByText(/Open faulty index/);
        expect(openFaultyButtons.length).toBeGreaterThan(0);

        expect(screen.queryByText(/Set State/)).not.toBeInTheDocument();
    });

    it("can open faulty index - single node", async () => {
        const { screen } = rtlRender(<FaultyIndexSingleNode />);

        const openFaultyButtons = await screen.findAllByText(/Open faulty index/);
        expect(openFaultyButtons).toHaveLength(2);

        expect(screen.queryByText(/Set State/)).not.toBeInTheDocument();
    });

    it("can show community limits", async () => {
        const { screen } = rtlRender(<CommunityLimits />);

        expect(await screen.findByText(/Cluster is reaching/)).toBeInTheDocument();
        expect(screen.getByText(/Database is reaching/)).toBeInTheDocument();
    });
});
