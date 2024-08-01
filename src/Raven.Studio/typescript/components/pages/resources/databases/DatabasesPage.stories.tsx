import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryFn, StoryObj } from "@storybook/react";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import React from "react";
import { DatabasesPage } from "./DatabasesPage";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabaseSharedInfo } from "components/models/databases";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Databases",
    component: DatabasesPage,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DatabasesPage>;

function commonInit() {
    const { cluster } = mockStore;
    cluster.with_Cluster();

    const { accessManager } = mockStore;
    accessManager.with_securityClearance("ClusterAdmin");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

function getDatabaseNamesForNode(nodeTag: string, dto: DatabaseSharedInfo): string[] {
    if (dto.isSharded) {
        return dto.shards.map((x) => (x.nodes.some((n) => n.tag === nodeTag) ? x.name : null)).filter((x) => x);
    }

    return dto.nodes.some((x) => x.tag === nodeTag) ? [dto.name] : [];
}

export const Sharded: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Sharded();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return <DatabasesPage />;
};

export const Cluster: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Cluster();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return <DatabasesPage />;
};

export const WithDifferentAccessLevel: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const { accessManager } = mockStore;
    accessManager.with_securityClearance("ValidUser");

    const dbAccess: dictionary<databaseAccessLevel> = {
        admin: "DatabaseAdmin",
        read_write: "DatabaseReadWrite",
        read_only: "DatabaseRead",
    };

    accessManager.with_databaseAccess(dbAccess);

    const adminDb = DatabasesStubs.nonShardedClusterDatabase();
    adminDb.name = "admin";

    const readWriteDb = DatabasesStubs.nonShardedClusterDatabase();
    readWriteDb.name = "read_write";

    const readDb = DatabasesStubs.nonShardedClusterDatabase();
    readDb.name = "read_only";

    mockStore.databases.withDatabases([adminDb.toDto(), readWriteDb.toDto(), readDb.toDto()]);

    mockServices.databasesService.withGetDatabasesState(() => Object.keys(dbAccess));

    return <DatabasesPage />;
};

export const WithLoadErrorOnSingleNode: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Cluster();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value), {
        loadError: ["B"],
    });

    return <DatabasesPage />;
};

export const WithLoadErrorOnAllNodes: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Cluster();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value), {
        loadError: ["A", "B", "C"],
    });

    return <DatabasesPage />;
};

export const WithDeletion: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Cluster((x) => {
        x.deletionInProgress = ["Z"];
    });

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return <DatabasesPage />;
};

export const Single: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Single();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return <DatabasesPage />;
};

export const CompactDatabaseAuto: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Single();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return <DatabasesPage compact={value.name} />;
};

function assignNodeType(tag: string): databaseGroupNodeType {
    switch (tag) {
        case "B":
            return "Promotable";
        case "C":
            return "Rehab";
        default:
            return "Member";
    }
}

export const DifferentNodeStates: StoryFn<typeof DatabasesPage> = () => {
    commonInit();

    const clusterDb = DatabasesStubs.nonShardedClusterDatabase().toDto();
    clusterDb.nodes.forEach((n) => (n.type = assignNodeType(n.tag)));

    const shardedDb = DatabasesStubs.shardedDatabase().toDto();
    shardedDb.nodes.forEach((n) => (n.type = assignNodeType(n.tag)));
    shardedDb.shards.forEach((s) => {
        s.nodes.forEach((n) => {
            n.type = assignNodeType(n.tag);
        });
    });

    mockStore.databases.withDatabases([clusterDb, shardedDb]);

    mockServices.databasesService.withGetDatabasesState(() => [clusterDb.name, ...shardedDb.shards.map((x) => x.name)]);

    return <DatabasesPage />;
};

export const WithOfflineNodes: StoryObj<{ offlineNodes: string[] }> = {
    render: ({ offlineNodes }: { offlineNodes: string[] }) => {
        commonInit();

        const value = mockStore.databases.with_Cluster();

        mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value), {
            offlineNodes: offlineNodes,
        });

        return <DatabasesPage />;
    },
    args: {
        offlineNodes: ["A"],
    },
};
