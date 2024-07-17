import React from "react";
import { Meta, StoryFn } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientGlobalConfiguration from "./ClientGlobalConfiguration";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/ManageServer/Client Configuration",
    component: ClientGlobalConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ClientGlobalConfiguration>;

function commonInit() {
    const { manageServerService } = mockServices;
    manageServerService.withGetGlobalClientConfiguration();
}

export const ClientConfiguration: StoryFn<typeof ClientGlobalConfiguration> = () => {
    commonInit();

    const { license } = mockStore;
    license.with_License();

    return <ClientGlobalConfiguration />;
};

export const LicenseRestricted: StoryFn<typeof ClientGlobalConfiguration> = () => {
    commonInit();

    const { license } = mockStore;
    license.with_LicenseLimited({ HasClientConfiguration: false });

    return <ClientGlobalConfiguration />;
};
