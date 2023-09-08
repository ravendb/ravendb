import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientGlobalConfiguration from "./ClientGlobalConfiguration";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer/Client Configuration",
    component: ClientGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof ClientGlobalConfiguration>;

function commonInit() {
    const { manageServerService } = mockServices;
    manageServerService.withGetGlobalClientConfiguration();
}

export const ClientConfiguration: ComponentStory<typeof ClientGlobalConfiguration> = () => {
    commonInit();

    const { license } = mockStore;
    license.with_Enterprise();

    return <ClientGlobalConfiguration />;
};

export const LicenseRestricted: ComponentStory<typeof ClientGlobalConfiguration> = () => {
    commonInit();

    const { license } = mockStore;
    license.with_Community();

    return <ClientGlobalConfiguration />;
};
