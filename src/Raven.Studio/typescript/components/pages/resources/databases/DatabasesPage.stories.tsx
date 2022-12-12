import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "test/mocks/services/MockServices";
import React from "react";
import { DatabasesPage } from "./DatabasesPage";

export default {
    title: "Pages/Databases",
    component: DatabasesPage,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof DatabasesPage>;

export const Sharded: ComponentStory<typeof DatabasesPage> = () => {
    accessManager.default.securityClearance("ClusterAdmin");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { databasesService } = mockServices;

    databasesService.withGetDatabases_Sharded();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <DatabasesPage />
        </div>
    );
};

export const Single: ComponentStory<typeof DatabasesPage> = () => {
    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    const { databasesService } = mockServices;

    databasesService.withGetDatabases_Single();

    return (
        <div style={{ height: "100vh", overflow: "auto" }}>
            <DatabasesPage />
        </div>
    );
};
