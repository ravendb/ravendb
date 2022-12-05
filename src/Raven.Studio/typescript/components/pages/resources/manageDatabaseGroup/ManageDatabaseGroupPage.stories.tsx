import { withBootstrap5, withStorybookContexts } from "../../../../test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { ManageDatabaseGroupPage } from "components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage";
import { mockServices } from "../../../../test/mocks/MockServices";
import React from "react";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { DatabasesStubs } from "../../../../test/stubs/DatabasesStubs";

export default {
    title: "Pages/Manage Database Group",
    component: ManageDatabaseGroupPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ManageDatabaseGroupPage>;

function commonInit() {
    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
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
