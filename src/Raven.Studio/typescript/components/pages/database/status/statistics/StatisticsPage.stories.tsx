import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ComponentMeta } from "@storybook/react";
import { StatisticsPage } from "./StatisticsPage";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import React from "react";
import database from "models/resources/database";
import { boundCopy } from "components/utils/common";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import { IndexesStubs } from "test/stubs/IndexesStubs";

export default {
    title: "Pages/Statistics page",
    component: StatisticsPage,
    decorators: [withStorybookContexts, withBootstrap5],
    excludeStories: /Template$/,
} as ComponentMeta<typeof StatisticsPage>;

export const StatisticsTemplate = (args: { db: database; stats?: IndexStats[] }) => {
    const { databasesService, indexesService } = mockServices;

    databasesService.withEssentialStats();

    databasesService.withDetailedStats();

    indexesService.withGetStats(args.stats);

    return <StatisticsPage db={args.db} />;
};

export const StatsSingleNode = boundCopy(StatisticsTemplate, {
    db: DatabasesStubs.nonShardedSingleNodeDatabase(),
});

export const StatsSharded = boundCopy(StatisticsTemplate, {
    db: DatabasesStubs.shardedDatabase(),
});

export const FaultySupport = boundCopy(StatisticsTemplate, {
    db: DatabasesStubs.shardedDatabase(),
    stats: IndexesStubs.getSampleStats().map((x) => {
        x.Type = "Faulty";
        return x;
    }),
});
