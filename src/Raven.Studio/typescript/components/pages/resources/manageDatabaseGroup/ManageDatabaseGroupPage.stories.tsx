import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { ManageDatabaseGroupPage } from "components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage";
import { mockServices } from "test/mocks/services/MockServices";
import React from "react";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { ClusterStubs } from "test/stubs/ClusterStubs";
import licenseModel from "models/auth/licenseModel";

export default {
    title: "Pages/Manage Database Group",
    component: ManageDatabaseGroupPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ManageDatabaseGroupPage>;

function commonInit() {
    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.topology(ClusterStubs.singleNodeTopology());
    licenseModel.licenseStatus({
        HasDynamicNodesDistribution: true,
    } as any);
}

export const SingleNode: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Single();

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const NotAllNodesUsed: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();
    clusterTopologyManager.default.topology(ClusterStubs.clusterTopology());

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Single();

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const Cluster: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Cluster();

    const db = DatabasesStubs.nonShardedClusterDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};

export const ClusterWithDeletion: ComponentStory<typeof ManageDatabaseGroupPage> = () => {
    commonInit();

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Cluster((x) => {
        x.DeletionInProgress = {
            HARD: "HardDelete",
            SOFT: "SoftDelete",
            ZZZZ: "HardDelete",
        };
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

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Cluster((x) => {
        const status = x.NodesTopology.Status["A"];
        status.LastStatus = "HighDirtyMemory";
        status.LastError = "This is some node error";
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

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Single((x) => {
        x.LockMode = "PreventDeletesIgnore";
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

    const { databasesService } = mockServices;
    databasesService.withGetDatabase_Single((x) => {
        x.LockMode = "PreventDeletesError";
    });

    const db = DatabasesStubs.nonShardedSingleNodeDatabase();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <ManageDatabaseGroupPage db={db} />
        </div>
    );
};
