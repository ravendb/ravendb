import React from "react";
import { ComponentMeta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientDatabaseConfiguration from "./ClientDatabaseConfiguration";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings/ClientConfiguration",
    component: ClientDatabaseConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientDatabaseConfiguration>;

export function ClientConfiguration() {
    const { manageServerService } = mockServices;

    manageServerService.withGetGlobalClientConfiguration();
    manageServerService.withGetDatabaseClientConfiguration();

    return <ClientDatabaseConfiguration db={DatabasesStubs.nonShardedSingleNodeDatabase()} />;
}
