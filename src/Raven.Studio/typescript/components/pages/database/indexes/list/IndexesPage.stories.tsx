import { IndexesPage } from "./IndexesPage";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Indexes/List of indexes",
    component: IndexesPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof IndexesPage>;

function commonInit() {
    const { accessManager, license } = mockStore;
    accessManager.with_securityClearance("ClusterAdmin");
    license.with_Enterprise();

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

function configureIndexService() {
    const { indexesService } = mockServices;

    indexesService.withGetSampleStats();
    indexesService.withGetProgress();
}

function configureDifferentIndexStates() {
    const { indexesService } = mockServices;

    const [upToDateStats, upToDateProgress] = IndexesStubs.getUpToDateIndex();
    const [upToDateStatsWithErrors, upToDateProgressWithErrors] = IndexesStubs.getUpToDateIndexWithErrors();
    const [staleStats, staleProgress] = IndexesStubs.getStaleInProgressIndex();
    const [disabledStats1, disabledProgress1] = IndexesStubs.getDisabledIndexWithProgress();
    const [pausedStats1, pausedProgress1] = IndexesStubs.getPausedIndexWithProgress();
    const [disabledStats2, disabledProgress2] = IndexesStubs.getDisabledIndexWithOutProgress();
    const [pausedStats2, pausedProgress2] = IndexesStubs.getPausedIndexWithOutProgress();
    const [faultyStats, faultyProgress] = IndexesStubs.getFaultyIndex();
    const [erroredStats, erroredProgress] = IndexesStubs.getErroredIndex();

    indexesService.withGetStats(
        [
            upToDateStats,
            upToDateStatsWithErrors,
            staleStats,
            disabledStats1,
            pausedStats1,
            disabledStats2,
            pausedStats2,
            faultyStats,
            erroredStats,
        ].filter((x) => x)
    );
    indexesService.withGetProgress(
        [
            upToDateProgress,
            upToDateProgressWithErrors,
            staleProgress,
            disabledProgress1,
            pausedProgress1,
            disabledProgress2,
            pausedProgress2,
            faultyProgress,
            erroredProgress,
        ].filter((x) => x)
    );
}

export const EmptyView: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    commonInit();

    const { indexesService } = mockServices;

    indexesService.withGetStats((dto) => {
        dto.length = 0;
    });
    indexesService.withGetProgress((dto) => {
        dto.length = 0;
    });

    return <IndexesPage db={db} />;
};

export const SampleDataSingleNode: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    commonInit();
    configureIndexService();

    return <IndexesPage db={db} />;
};

export const SampleDataCluster: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedClusterDatabase();

    commonInit();
    configureIndexService();

    return <IndexesPage db={db} />;
};

export const SampleDataSharded: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();
    configureIndexService();

    return <IndexesPage db={db} />;
};

export const DifferentIndexNodeStatesSingleNode: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    commonInit();
    configureDifferentIndexStates();

    return <IndexesPage db={db} />;
};

export const DifferentIndexNodeStatesSharded: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();
    configureDifferentIndexStates();

    return <IndexesPage db={db} />;
};

export const FaultyIndexSingleNode: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    commonInit();
    const { indexesService } = mockServices;

    const [faultyStats] = IndexesStubs.getFaultyIndex();

    indexesService.withGetStats([faultyStats].filter((x) => x));
    indexesService.withGetProgress([]);

    return <IndexesPage db={db} />;
};

export const FaultyIndexSharded: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();
    const { indexesService } = mockServices;

    const [faultyStats] = IndexesStubs.getFaultyIndex();

    indexesService.withGetStats([faultyStats].filter((x) => x));
    indexesService.withGetProgress([]);

    return <IndexesPage db={db} />;
};

export const LicenseLimits: ComponentStory<typeof IndexesPage> = () => {
    const db = DatabasesStubs.shardedDatabase();
    const { license } = mockStore;

    commonInit();
    configureDifferentIndexStates();

    license.with_Community();
    license.with_LimitsUsage();

    return <IndexesPage db={db} />;
};
