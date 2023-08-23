import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientGlobalConfiguration from "./ClientGlobalConfiguration";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/ManageServer/Client Configuration",
    component: ClientGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientGlobalConfiguration>;

export const ClientConfiguration: ComponentStory<typeof ClientGlobalConfiguration> = () => {
    const { manageServerService } = mockServices;

    manageServerService.withGetGlobalClientConfiguration();

    return <ClientGlobalConfiguration />;
};

export const LicenseRestricted: ComponentStory<typeof ClientGlobalConfiguration> = () => {
    const { manageServerService } = mockServices;

    manageServerService.withGetGlobalClientConfiguration();

    return <ClientGlobalConfiguration licenseType="community" />;
};
