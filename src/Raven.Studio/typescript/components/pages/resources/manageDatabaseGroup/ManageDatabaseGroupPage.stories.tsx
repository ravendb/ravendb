import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryFn } from "@storybook/react";
import { ManageDatabaseGroupPage } from "components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage";
import React from "react";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { ClusterStubs } from "test/stubs/ClusterStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Settings/Manage Database Group",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

function commonInit() {
    const { licenseService } = mockServices;
    licenseService.withLimitsUsage();

    const { accessManager, license, cluster } = mockStore;

    accessManager.with_securityClearance("ClusterAdmin");
    license.with_License();
    cluster.with_Single();
}

export const SingleNode: StoryFn = () => {
    commonInit();

    mockStore.databases.withActiveDatabase_NonSharded_SingleNode();

    return <ManageDatabaseGroupPage />;
};

export const NotAllNodesUsed: StoryFn = () => {
    // needed for old inner component (add node dialog)
    clusterTopologyManager.default.topology(ClusterStubs.clusterTopology());

    commonInit();

    const { cluster, databases } = mockStore;

    cluster.with_Cluster();
    databases.withActiveDatabase_NonSharded_SingleNode();

    return <ManageDatabaseGroupPage />;
};

export const Cluster: StoryFn = () => {
    commonInit();

    mockStore.databases.withActiveDatabase_NonSharded_Cluster((x) => (x.nodes[0].type = "Promotable"));

    return <ManageDatabaseGroupPage />;
};

export const Sharded: StoryFn = () => {
    commonInit();

    const { cluster, databases } = mockStore;

    cluster.with_Cluster();
    databases.withActiveDatabase_Sharded((x) => (x.shards[0].nodes[0].type = "Promotable"));

    return <ManageDatabaseGroupPage />;
};

export const ClusterWithDeletion: StoryFn = () => {
    commonInit();

    mockStore.databases.withActiveDatabase_NonSharded_Cluster((x) => {
        x.deletionInProgress = ["HARD", "SOFT"];
    });

    return <ManageDatabaseGroupPage />;
};

export const ClusterWithFailure: StoryFn = () => {
    commonInit();

    mockStore.databases.withActiveDatabase_NonSharded_Cluster((x) => {
        x.nodes[0].lastStatus = "HighDirtyMemory";
        x.nodes[0].lastError = "This is some node error, which might be quite long in some cases...";
        x.nodes[0].responsibleNode = "X";
        x.nodes[0].type = "Rehab";
    });

    return <ManageDatabaseGroupPage />;
};

export const PreventDeleteIgnore: StoryFn = () => {
    commonInit();

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();

    return <ManageDatabaseGroupPage />;
};

export const PreventDeleteError: StoryFn = () => {
    commonInit();

    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode((x) => {
        x.lockMode = "PreventDeletesError";
    });

    return <ManageDatabaseGroupPage />;
};
