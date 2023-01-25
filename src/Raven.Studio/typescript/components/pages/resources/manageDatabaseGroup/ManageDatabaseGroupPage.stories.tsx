import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { ManageDatabaseGroupPage } from "components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage";
import React from "react";
import accessManager from "common/shell/accessManager";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import licenseModel from "models/auth/licenseModel";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { ClusterStubs } from "test/stubs/ClusterStubs";

export default {
    title: "Pages/Manage Database Group",
    component: ManageDatabaseGroupPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ManageDatabaseGroupPage>;

function commonInit() {
    accessManager.default.securityClearance("ClusterAdmin");

    const { useClusterTopologyManager } = mockHooks;
    useClusterTopologyManager.with_Single();

    licenseModel.licenseStatus({
        HasDynamicNodesDistribution: true,
    } as any);
}

export const SingleNode: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    const { useDatabaseManager } = mockHooks;
    useDatabaseManager.withDatabases([db.toDto()]);

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const NotAllNodesUsed: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    // needed for old inner component (add node dialog)
    clusterTopologyManager.default.topology(ClusterStubs.clusterTopology());

    commonInit();

    const { useClusterTopologyManager, useDatabaseManager } = mockHooks;

    useClusterTopologyManager.with_Cluster();
    useDatabaseManager.with_Single();

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const Cluster: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useDatabaseManager } = mockHooks;
    useDatabaseManager.with_Cluster();

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const Sharded: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useDatabaseManager, useClusterTopologyManager } = mockHooks;

    useDatabaseManager.with_Sharded();
    useClusterTopologyManager.with_Cluster();

    const db = DatabasesStubs.shardedDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const ClusterWithDeletion: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useDatabaseManager } = mockHooks;
    useDatabaseManager.with_Cluster((x) => {
        x.deletionInProgress = ["HARD", "SOFT"];
    });

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const ClusterWithFailure: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useDatabaseManager } = mockHooks;
    useDatabaseManager.with_Cluster((x) => {
        x.nodes[0].lastStatus = "HighDirtyMemory";
        x.nodes[0].lastError = "This is some node error, which might be quite long in some cases...";
        x.nodes[0].responsibleNode = "X";
    });

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const PreventDeleteIgnore: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useDatabaseManager } = mockHooks;
    useDatabaseManager.with_Single((x) => {
        x.lockMode = "PreventDeletesIgnore";
    });

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const PreventDeleteError: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { useDatabaseManager } = mockHooks;
    useDatabaseManager.with_Single((x) => {
        x.lockMode = "PreventDeletesError";
    });

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};
