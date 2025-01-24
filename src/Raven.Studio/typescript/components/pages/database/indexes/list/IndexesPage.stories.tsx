import { IndexesPage } from "./IndexesPage";
import { Meta, StoryFn } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Indexes/List of Indexes",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

function commonInit() {
    const { accessManager, license } = mockStore;
    const { licenseService } = mockServices;

    accessManager.with_securityClearance("ClusterAdmin");
    license.with_License();

    licenseService.withLimitsUsage();

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

export const EmptyView: StoryFn = () => {
    commonInit();

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();

    const { indexesService } = mockServices;

    indexesService.withGetStats((dto) => {
        dto.length = 0;
    });
    indexesService.withGetProgress((dto) => {
        dto.length = 0;
    });

    return <IndexesPage />;
};

export const SampleDataSingleNode: StoryFn = () => {
    commonInit();
    configureIndexService();

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();

    return <IndexesPage />;
};

export const SampleDataCluster: StoryFn = () => {
    commonInit();
    configureIndexService();

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_Cluster();

    return <IndexesPage />;
};

export const SampleDataSharded: StoryFn = () => {
    commonInit();
    configureIndexService();

    const { databases } = mockStore;
    databases.withActiveDatabase_Sharded();

    return <IndexesPage />;
};

export const DifferentIndexNodeStatesSingleNode: StoryFn = () => {
    commonInit();
    configureDifferentIndexStates();

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();

    return <IndexesPage />;
};

export const DifferentIndexNodeStatesSharded: StoryFn = () => {
    commonInit();
    configureDifferentIndexStates();

    const { databases } = mockStore;
    databases.withActiveDatabase_Sharded();

    return <IndexesPage />;
};

export const FaultyIndexSingleNode: StoryFn = () => {
    commonInit();
    const { indexesService } = mockServices;

    const [faultyStats] = IndexesStubs.getFaultyIndex();

    indexesService.withGetStats([faultyStats].filter((x) => x));
    indexesService.withGetProgress([]);

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();

    return <IndexesPage />;
};

export const FaultyIndexSharded: StoryFn = () => {
    commonInit();
    const { indexesService } = mockServices;

    const [faultyStats] = IndexesStubs.getFaultyIndex();

    indexesService.withGetStats([faultyStats].filter((x) => x));
    indexesService.withGetProgress([]);

    const { databases } = mockStore;
    databases.withActiveDatabase_Sharded();

    return <IndexesPage />;
};

export const LicenseLimits: StoryFn = () => {
    commonInit();
    configureDifferentIndexStates();

    const { databases, license } = mockStore;

    license.with_LicenseLimited();
    license.with_LimitsUsage();

    databases.withActiveDatabase_Sharded();

    return <IndexesPage />;
};
