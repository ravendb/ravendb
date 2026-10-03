import { rtlRender, waitFor } from "test/rtlTestUtils";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import messagePublisher from "common/messagePublisher";
import IndexesService from "components/services/IndexesService";
import { composeStories } from "@storybook/react-webpack5";

import * as stories from "./IndexesPage.stories";

const {
    EmptyView,
    SampleDataCluster,
    FaultyIndexSharded,
    FaultyIndexSingleNode,
    LicenseLimits: CommunityLimits,
    StaleIndexWithEstimatedProgress,
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

    it("can request the exact progress of a stale index", async () => {
        const { screen, fireClick, user } = rtlRender(<StaleIndexWithEstimatedProgress />);

        await screen.findByText("StaleInProgress");

        const getProgress = mockServices.indexesService.mock.getProgress as unknown as jest.Mock;
        await waitFor(() => expect(getProgress).toHaveBeenCalled());

        // the periodic refresh is not scoped to any index, so the server reports every stale index
        expect(getProgress).toHaveBeenLastCalledWith(expect.any(String), expect.anything());

        const distributionItems = screen.queryAllByClassName("distribution-item");
        expect(distributionItems.length).toBeGreaterThan(0);
        await user.hover(distributionItems[0]);

        expect(await screen.findByText("(~)")).toBeInTheDocument();

        await fireClick(await screen.findByText("show exact counts"));

        await waitFor(() =>
            expect(getProgress).toHaveBeenLastCalledWith(
                expect.any(String),
                expect.anything(),
                ["StaleInProgress"],
                true
            )
        );
        expect(await screen.findByText("show estimated counts")).toBeInTheDocument();
    });

    it("keeps the estimated counts when the exact progress request fails", async () => {
        const reportError = jest.spyOn(messagePublisher, "reportError").mockImplementation(() => {});
        const getProgress = mockServices.indexesService.mock.getProgress as unknown as jest.Mock;

        try {
            const { screen, fireClick, user } = rtlRender(<StaleIndexWithEstimatedProgress />);

            await screen.findByText("StaleInProgress");

            const [, staleProgress] = IndexesStubs.getStaleInProgressIndex();
            getProgress.mockImplementation((...args: Parameters<IndexesService["getProgress"]>) => {
                const [, , , exact] = args;
                return exact ? Promise.reject({ responseText: "timeout" }) : Promise.resolve([staleProgress]);
            });

            const distributionItems = screen.queryAllByClassName("distribution-item");
            await user.hover(distributionItems[0]);

            await fireClick(await screen.findByText("show exact counts"));

            await waitFor(() => expect(reportError).toHaveBeenCalled());

            // the toggle is reverted, so the link still offers the exact counts
            expect(await screen.findByText("show exact counts")).toBeInTheDocument();
            expect(screen.queryByText("show estimated counts")).not.toBeInTheDocument();
        } finally {
            reportError.mockRestore();
            getProgress.mockReset();
        }
    });
});
