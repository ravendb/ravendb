import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientDatabaseConfiguration from "./ClientDatabaseConfiguration";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/ClientConfiguration",
    component: ClientDatabaseConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientDatabaseConfiguration>;

function commonInit() {
    const { accessManager } = mockStore;
    const { manageServerService } = mockServices;

    accessManager.with_securityClearance("ClusterAdmin");
    manageServerService.withGetDatabaseClientConfiguration();
}

export const WithGlobalConfiguration: ComponentStory<typeof ClientDatabaseConfiguration> = () => {
    commonInit();

    const { manageServerService } = mockServices;
    manageServerService.withGetGlobalClientConfiguration();

    return <ClientDatabaseConfiguration db={DatabasesStubs.nonShardedSingleNodeDatabase()} />;
};

export const WithoutGlobalConfiguration: ComponentStory<typeof ClientDatabaseConfiguration> = () => {
    commonInit();

    const { manageServerService } = mockServices;
    manageServerService.withThrowingGetGlobalClientConfiguration();

    return <ClientDatabaseConfiguration db={DatabasesStubs.nonShardedSingleNodeDatabase()} />;
};

export const LicenseRestricted: ComponentStory<typeof ClientDatabaseConfiguration> = () => {
    commonInit();

    const { manageServerService } = mockServices;
    const { license } = mockStore;
    manageServerService.withGetGlobalClientConfiguration();
    license.with_Community();

    return <ClientDatabaseConfiguration db={DatabasesStubs.nonShardedSingleNodeDatabase()} />;
};
