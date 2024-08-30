import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { act, rtlRender } from "test/rtlTestUtils";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { composeStory } from "@storybook/react";
import { boundCopy } from "components/utils/common";
import * as stories from "../../status/statistics/StatisticsPage.stories";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { mockStore } from "test/mocks/store/MockStore";

const selectors = {
    detailedStatsHeader: /Detailed Database Stats/i,
    documentsCount: /Documents Count/,
    showDetails: /show detailed/i,
    detailedIndexHeader: /Indexes Stats/i,
    noIndexes: /No indexes have been created for this database/i,
};

describe("StatisticsPage", function () {
    beforeEach(() => {
        jest.resetAllMocks();
        const { accessManager, cluster } = mockStore;
        accessManager.with_securityClearance("ClusterAdmin");
        cluster.with_Single();
    });

    it("can render stats w/o details", async () => {
        const { databasesService } = mockServices;

        const stats = databasesService.withEssentialStats();
        const View = boundCopy(stories.StatisticsTemplate, {
            db: DatabasesStubs.shardedDatabase().toDto(),
        });

        const Story = composeStory(View, stories.default);

        const { screen } = rtlRender(<Story />);

        expect(await screen.findByText(selectors.documentsCount)).toBeInTheDocument();
        expect(await screen.findByText(stats.CountOfIndexes)).toBeInTheDocument();
    });

    it("can render stats w/ details", async () => {
        const View = boundCopy(stories.StatisticsTemplate, {
            db: DatabasesStubs.shardedDatabase().toDto(),
        });

        const Story = composeStory(View, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        expect(await screen.findByText(selectors.documentsCount)).toBeInTheDocument();

        await fireClick(screen.queryByText(selectors.showDetails));

        expect(await screen.findByText(selectors.detailedStatsHeader)).toBeInTheDocument();
        expect(await screen.findByText(selectors.detailedIndexHeader)).toBeInTheDocument();
    });

    it("can render index map stats", async () => {
        const db = DatabasesStubs.shardedDatabase().toDto();
        const ordersStats = IndexesStubs.getSampleStats().find((x) => x.Name === "Orders/ByShipment/Location");
        ordersStats.MapErrors = 5;

        // we display rate if > 1
        ordersStats.ReducedPerSecondRate = 0.5;
        ordersStats.MappedPerSecondRate = 0.2;

        const View = boundCopy(stories.StatisticsTemplate, {
            db,
            stats: [ordersStats],
        });

        const Story = composeStory(View, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const detailsBtn = await screen.findByText(selectors.showDetails);
        await fireClick(detailsBtn);

        expect(screen.queryByText("Entries Count")).toBeInTheDocument();
        expect(screen.queryByText("Map Attempts")).toBeInTheDocument();
        expect(screen.queryByText("Map Successes")).toBeInTheDocument();
        expect(screen.queryByText("Map Errors")).toBeInTheDocument();

        expect(screen.queryByText("Reduce Attempts")).not.toBeInTheDocument();
        expect(screen.queryByText("Reduce Successes")).not.toBeInTheDocument();

        expect(screen.queryByText("Mapped Per Second Rate")).not.toBeInTheDocument();
        expect(screen.queryByText("Reduced Per Second Rate")).not.toBeInTheDocument();
    });

    it("can render index map stats", async () => {
        const db = DatabasesStubs.shardedDatabase().toDto();
        const productRating = IndexesStubs.getSampleStats().find((x) => x.Name === "Product/Rating");
        productRating.MapErrors = 5;
        productRating.ReduceErrors = 2;

        productRating.MappedPerSecondRate = 27;
        productRating.ReducedPerSecondRate = 62;

        const View = boundCopy(stories.StatisticsTemplate, {
            db,
            stats: [productRating],
        });

        const Story = composeStory(View, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const detailsBtn = await screen.findByText(selectors.showDetails);
        await fireClick(detailsBtn);

        expect(screen.queryByText("Entries Count")).toBeInTheDocument();
        expect(screen.queryByText("Map Attempts")).toBeInTheDocument();
        expect(screen.queryByText("Map Successes")).toBeInTheDocument();
        expect(screen.queryByText("Map Errors")).toBeInTheDocument();

        expect(screen.queryByText("Reduce Attempts")).toBeInTheDocument();
        expect(screen.queryByText("Reduce Successes")).toBeInTheDocument();
        expect(screen.queryByText("Reduce Errors")).toBeInTheDocument();

        expect(screen.queryByText("Mapped Per Second Rate")).toBeInTheDocument();
        expect(screen.queryByText("Reduced Per Second Rate")).toBeInTheDocument();
    });

    it("can handle no indexes case", async () => {
        const db = DatabasesStubs.shardedDatabase().toDto();

        const View = boundCopy(stories.StatisticsTemplate, {
            db,
            stats: [],
        });

        const Story = composeStory(View, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const detailsBtn = await screen.findByText(selectors.showDetails);
        await fireClick(detailsBtn);

        expect(await screen.findByText(selectors.noIndexes)).toBeInTheDocument();
    });

    it("can stay with open details after database state change", async () => {
        const db = DatabasesStubs.nonShardedSingleNodeDatabase().toDto();
        const View = boundCopy(stories.StatisticsTemplate, {
            db,
        });

        const Story = composeStory(View, stories.default);
        const { screen, fireClick } = rtlRender(<Story />);

        await fireClick(screen.queryByText(selectors.showDetails));

        // details are visible
        expect(await screen.findByText(selectors.detailedIndexHeader)).toBeInTheDocument();

        // changing state of active database
        act(() => {
            const { databases } = mockStore;
            databases.withActiveDatabase({ ...db, indexesCount: db.indexesCount - 1 });
        });

        // details are still visible
        expect(await screen.findByText(selectors.detailedIndexHeader)).toBeInTheDocument();
    });
});
