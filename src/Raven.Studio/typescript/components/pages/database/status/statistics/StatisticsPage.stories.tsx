import { withStorybookContexts } from "../../../../../test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { StatisticsPage } from "./StatisticsPage";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "../../../../../test/mocks/MockServices";
import React from "react";

export default {
    title: "Statistics page",
    component: StatisticsPage,
    decorators: [withStorybookContexts],
} as ComponentMeta<typeof StatisticsPage>;

export const StatsSingleNode: ComponentStory<typeof StatisticsPage> = () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { databasesService } = mockServices;

    databasesService.withEssentialStats();
    databasesService.withDetailedStats();

    return (
        <div className="content-margin absolute-fill stats">
            <StatisticsPage database={db} />
        </div>
    );
};

export const StatsSharded: ComponentStory<typeof StatisticsPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { databasesService } = mockServices;

    databasesService.withEssentialStats();
    databasesService.withDetailedStats();

    return (
        <div className="content-margin absolute-fill stats">
            <StatisticsPage database={db} />
        </div>
    );
};
