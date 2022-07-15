import { withStorybookContexts } from "../../../../../test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { StatisticsPage } from "./StatisticsPage";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "../../../../../test/mocks/MockServices";
import React from "react";
import database from "models/resources/database";
import { boundCopy } from "../../../../utils/common";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import { IndexesStubs } from "../../../../../test/stubs/IndexesStubs";

export default {
    title: "Pages/Statistics page",
    component: StatisticsPage,
    decorators: [withStorybookContexts],
    excludeStories: /Template$/,
} as ComponentMeta<typeof StatisticsPage>;

export const StatisticsTemplate = (args: { db: database; stats?: IndexStats[] }) => {
    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { databasesService, indexesService } = mockServices;

    databasesService.withEssentialStats();
    databasesService.withDetailedStats();

    indexesService.withGetStats(args.stats);

    return (
        <div className="content-margin absolute-fill stats" style={{ height: "100vh", overflow: "auto" }}>
            <StatisticsPage database={args.db} />
        </div>
    );
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
