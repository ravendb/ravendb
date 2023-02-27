import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import React from "react";
import { DatabasesPage } from "./DatabasesPage";
import { mockStore } from "test/mocks/store/MockStore";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "components/models/databases";

export default {
    title: "Pages/Databases",
    component: DatabasesPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof DatabasesPage>;

function commonInit() {
    const { useClusterTopologyManager } = mockHooks;
    useClusterTopologyManager.with_Cluster();

    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

function getDatabaseNamesForNode(nodeTag: string, dto: DatabaseSharedInfo): string[] {
    if (dto.sharded) {
        const shardedDto = dto as ShardedDatabaseSharedInfo;
        return shardedDto.shards.map((x) => (x.nodes.some((n) => n.tag === nodeTag) ? x.name : null)).filter((x) => x);
    }

    return dto.nodes.some((x) => x.tag === nodeTag) ? [dto.name] : [];
}

export const Sharded: ComponentStory<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Sharded();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <DatabasesPage />
        </div>
    );
};

export const Cluster: ComponentStory<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Cluster();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <DatabasesPage />
        </div>
    );
};

export const WithDeletion: ComponentStory<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Cluster((x) => {
        x.deletionInProgress = ["Z"];
    });

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <DatabasesPage />
        </div>
    );
};

export const Single: ComponentStory<typeof DatabasesPage> = () => {
    commonInit();

    const value = mockStore.databases.with_Single();

    mockServices.databasesService.withGetDatabasesState((tag) => getDatabaseNamesForNode(tag, value));

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <DatabasesPage />
        </div>
    );
};
