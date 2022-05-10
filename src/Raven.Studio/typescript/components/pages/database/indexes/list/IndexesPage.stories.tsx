import { IndexesPage } from "./IndexesPage";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import React from "react";
import { withStorybookContexts } from "../../../../../test/storybookTestUtils";
import { mockServices } from "../../../../../test/mocks/MockServices";
import accessManager from "common/shell/accessManager";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

function indexesHolder(storyFn: any) {
    return (
        <div className="indexes content-margin no-transition absolute-fill">
            {storyFn()}
        </div>
    );
}

export default {
    title: "Indexes page",
    component: IndexesPage,
    decorators: [withStorybookContexts, indexesHolder],
} as ComponentMeta<typeof IndexesPage>;

export const SampleDataSingleNode: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { indexesService } = mockServices;

    indexesService.withGetSampleStats();
    indexesService.withGetProgress();

    return (
        <IndexesPage database={db} />
    );
};

export const SampleDataCluster: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedClusterDatabase();

    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { indexesService } = mockServices;

    indexesService.withGetSampleStats();
    indexesService.withGetProgress();

    return (
        <IndexesPage database={db} />
    );
};

export const SampleDataSharded: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { indexesService } = mockServices;

    indexesService.withGetSampleStats();
    indexesService.withGetProgress();

    return (
        <IndexesPage database={db} />
    );
};
