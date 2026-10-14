import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import messagePublisher from "common/messagePublisher";
import IndexesService from "components/services/IndexesService";
import { composeStories } from "@storybook/react-webpack5";

import * as stories from "./IndexesPage.stories";
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;

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

    it("keeps the exact counts when a superseded exact progress request fails", async () => {
        jest.useFakeTimers({ advanceTimers: true });
        const reportError = jest.spyOn(messagePublisher, "reportError").mockImplementation(() => {});
        const getProgress = mockServices.indexesService.mock.getProgress as unknown as jest.Mock;

        try {
            const { screen, fireClick, user } = rtlRender(<StaleIndexWithEstimatedProgress />);

            await screen.findByText("StaleInProgress");

            const [, staleProgress] = IndexesStubs.getStaleInProgressIndex();
            let rejectToggleRequest: (reason: unknown) => void;
            let exactCalls = 0;
            getProgress.mockImplementation((...args: Parameters<IndexesService["getProgress"]>) => {
                const [, , , exact] = args;
                if (exact && exactCalls++ === 0) {
                    return new Promise((_, reject) => (rejectToggleRequest = reject));
                }
                return Promise.resolve([staleProgress]);
            });

            const distributionItems = screen.queryAllByClassName("distribution-item");
            await user.hover(distributionItems[0]);

            await fireClick(await screen.findByText("show exact counts"));

            act(() => {
                jest.advanceTimersByTime(15_000);
            });
            await waitFor(() => expect(exactCalls).toBe(2));

            await act(async () => rejectToggleRequest({ responseText: "timeout" }));

            expect(await screen.findByText("show estimated counts")).toBeInTheDocument();
            expect(reportError).not.toHaveBeenCalled();
        } finally {
            reportError.mockRestore();
            getProgress.mockReset();
            jest.useRealTimers();
        }
    });

    it("keeps the exact counts when a refresh started before the toggle completes after it", async () => {
        jest.useFakeTimers({ advanceTimers: true });
        const getProgress = mockServices.indexesService.mock.getProgress as unknown as jest.Mock;

        try {
            const { screen, fireClick, user } = rtlRender(<StaleIndexWithEstimatedProgress />);

            await screen.findByText("StaleInProgress");

            const [, estimatedProgress] = IndexesStubs.getStaleInProgressIndex();
            const [, exactProgress] = IndexesStubs.getStaleInProgressIndex();
            Object.values(exactProgress.Collections).forEach((x) => (x.Estimated = false));

            let resolveRefresh: (progress: IndexProgress[]) => void;
            getProgress.mockImplementation((...args: Parameters<IndexesService["getProgress"]>) => {
                const [, , , exact] = args;
                return exact ? Promise.resolve([exactProgress]) : new Promise((resolve) => (resolveRefresh = resolve));
            });

            act(() => {
                jest.advanceTimersByTime(15_000);
            });
            await waitFor(() => expect(resolveRefresh).toBeDefined());

            const distributionItems = screen.queryAllByClassName("distribution-item");
            await user.hover(distributionItems[0]);

            await fireClick(await screen.findByText("show exact counts"));
            expect(await screen.findByText("show estimated counts")).toBeInTheDocument();
            await waitFor(() => expect(screen.queryByText("(~)")).not.toBeInTheDocument());

            await act(async () => resolveRefresh([estimatedProgress]));

            expect(screen.queryByText("(~)")).not.toBeInTheDocument();
        } finally {
            getProgress.mockReset();
            jest.useRealTimers();
        }
    });

    it("falls back to the estimated counts when a periodic exact progress refresh fails", async () => {
        jest.useFakeTimers({ advanceTimers: true });
        const reportError = jest.spyOn(messagePublisher, "reportError").mockImplementation(() => {});
        const getProgress = mockServices.indexesService.mock.getProgress as unknown as jest.Mock;

        try {
            const { screen, fireClick, user } = rtlRender(<StaleIndexWithEstimatedProgress />);

            await screen.findByText("StaleInProgress");

            const distributionItems = screen.queryAllByClassName("distribution-item");
            await user.hover(distributionItems[0]);

            await fireClick(await screen.findByText("show exact counts"));
            expect(await screen.findByText("show estimated counts")).toBeInTheDocument();

            const [, staleProgress] = IndexesStubs.getStaleInProgressIndex();
            getProgress.mockImplementation((...args: Parameters<IndexesService["getProgress"]>) => {
                const [, , , exact] = args;
                return exact ? Promise.reject({ responseText: "timeout" }) : Promise.resolve([staleProgress]);
            });

            act(() => {
                jest.advanceTimersByTime(15_000);
            });

            await waitFor(() => expect(reportError).toHaveBeenCalled());

            expect(await screen.findByText("show exact counts")).toBeInTheDocument();
            expect(screen.queryByText(/Unable to load index status/)).not.toBeInTheDocument();
        } finally {
            reportError.mockRestore();
            getProgress.mockReset();
            jest.useRealTimers();
        }
    });
});
