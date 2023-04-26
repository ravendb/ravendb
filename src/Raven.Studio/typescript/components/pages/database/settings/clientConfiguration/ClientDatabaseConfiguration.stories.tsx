import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientDatabaseConfiguration from "./ClientDatabaseConfiguration";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings/ClientConfiguration",
    component: ClientDatabaseConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientDatabaseConfiguration>;

export const WithGlobalConfiguration: ComponentStory<typeof ClientDatabaseConfiguration> = () => {
    const { manageServerService } = mockServices;

    manageServerService.withGetGlobalClientConfiguration();
    manageServerService.withGetDatabaseClientConfiguration();

    return <ClientDatabaseConfiguration db={DatabasesStubs.nonShardedSingleNodeDatabase()} />;
};

export const WithoutGlobalConfiguration: ComponentStory<typeof ClientDatabaseConfiguration> = () => {
    const { manageServerService } = mockServices;

    manageServerService.withThrowingGetGlobalClientConfiguration();
    manageServerService.withGetDatabaseClientConfiguration();

    return <ClientDatabaseConfiguration db={DatabasesStubs.nonShardedSingleNodeDatabase()} />;
};
