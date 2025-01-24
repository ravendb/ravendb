import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta } from "@storybook/react";
import { StatisticsPage } from "./StatisticsPage";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import React from "react";
import { boundCopy } from "components/utils/common";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { DatabaseSharedInfo } from "components/models/databases";

export default {
    title: "Pages/Stats/Stats",
    component: StatisticsPage,
    decorators: [withStorybookContexts, withBootstrap5],
    excludeStories: /Template$/,
} satisfies Meta<typeof StatisticsPage>;

export const StatisticsTemplate = (args: { db: DatabaseSharedInfo; stats?: IndexStats[] }) => {
    const { databasesService, indexesService } = mockServices;
    const { databases } = mockStore;

    databases.withActiveDatabase(args.db);

    databasesService.withEssentialStats();

    databasesService.withDetailedStats();

    indexesService.withGetStats(args.stats);

    return <StatisticsPage />;
};

export const StatsSingleNode = boundCopy(StatisticsTemplate, {
    db: DatabasesStubs.nonShardedSingleNodeDatabase().toDto(),
});

export const StatsSharded = boundCopy(StatisticsTemplate, {
    db: DatabasesStubs.shardedDatabase().toDto(),
});

export const FaultySupport = boundCopy(StatisticsTemplate, {
    db: DatabasesStubs.shardedDatabase().toDto(),
    stats: IndexesStubs.getSampleStats().map((x) => {
        x.Type = "Faulty";
        return x;
    }),
});
