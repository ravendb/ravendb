import { IndexesPage } from "./IndexesPage";
import { Meta, StoryFn } from "@storybook/react-webpack5";
import React from "react";
import { DatabaseType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import assertUnreachable from "components/utils/assertUnreachable";

export default {
    title: "Pages/Indexes/List of Indexes",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/wSIR2FKhW7kVwB5fgGd0Yu/Pages---Indexes?node-id=0-1&t=qGnZmjtUc3YpZW9G-1",
        },
    },
} satisfies Meta;

function commonInit(databaseType: DatabaseType) {
    const { accessManager, license } = mockStore;
    const { licenseService } = mockServices;

    accessManager.with_securityClearance("ClusterAdmin");
    license.with_License();

    licenseService.withLimitsUsage();

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    configureActiveDatabase(databaseType);
}

function configureActiveDatabase(databaseType: DatabaseType) {
    const { databases } = mockStore;
    const { databasesService } = mockServices;

    const db = (() => {
        switch (databaseType) {
            case "singleNode":
                return databases.withActiveDatabase_NonSharded_SingleNode();
            case "cluster":
                return databases.withActiveDatabase_NonSharded_Cluster();
            case "sharded":
                return databases.withActiveDatabase_Sharded();
            default:
                return assertUnreachable(databaseType);
        }
    })();

    databasesService.withGetDatabasesStateForDatabase(db);
}

function configureIndexService() {
    const { indexesService } = mockServices;

    indexesService.withGetSampleStats();
    indexesService.withGetProgress();
}

function configureDifferentIndexStates() {
    const { indexesService } = mockServices;

    const [idleStats, idleProgress] = IndexesStubs.getIdleIndex();
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
            idleStats,
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
            idleProgress,
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
    commonInit("singleNode");

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
    commonInit("singleNode");
    configureIndexService();

    return <IndexesPage />;
};

export const SampleDataCluster: StoryFn = () => {
    commonInit("cluster");
    configureIndexService();

    return <IndexesPage />;
};

export const SampleDataSharded: StoryFn = () => {
    commonInit("sharded");
    configureIndexService();

    return <IndexesPage />;
};

export const DifferentIndexNodeStatesSingleNode: StoryFn = () => {
    commonInit("singleNode");
    configureDifferentIndexStates();

    return <IndexesPage />;
};

export const DifferentIndexNodeStatesSharded: StoryFn = () => {
    commonInit("sharded");
    configureDifferentIndexStates();

    return <IndexesPage />;
};

export const FaultyIndexSingleNode: StoryFn = () => {
    commonInit("singleNode");
    const { indexesService } = mockServices;

    const [faultyStats] = IndexesStubs.getFaultyIndex();

    indexesService.withGetStats([faultyStats].filter((x) => x));
    indexesService.withGetProgress([]);

    return <IndexesPage />;
};

export const FaultyIndexSharded: StoryFn = () => {
    commonInit("sharded");
    const { indexesService } = mockServices;

    const [faultyStats] = IndexesStubs.getFaultyIndex();

    indexesService.withGetStats([faultyStats].filter((x) => x));
    indexesService.withGetProgress([]);

    return <IndexesPage />;
};

export const LicenseLimits: StoryFn = () => {
    commonInit("sharded");
    configureDifferentIndexStates();

    const { license } = mockStore;

    license.with_LicenseLimited();
    license.with_LimitsUsage();

    return <IndexesPage />;
};
