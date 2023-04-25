import React from "react";
import { ComponentMeta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientGlobalConfiguration from "./ClientGlobalConfiguration";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/ManageServer/ClientConfiguration",
    component: ClientGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientGlobalConfiguration>;

export function ClientConfiguration() {
    const { manageServerService } = mockServices;

    manageServerService.withGetGlobalClientConfiguration();

    return <ClientGlobalConfiguration />;
}
