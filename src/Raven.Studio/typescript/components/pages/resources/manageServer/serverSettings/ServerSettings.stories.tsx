import { Meta, StoryObj } from "@storybook/react";
import React from "react";
import { securityClearanceArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ServerSettings } from "./ServerSettings";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/ManageServer/Server Settings",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface DefaultServerSettingsProps {
    securityClearance: securityClearance;
    serverSettings: { Settings: Raven.Server.Config.ConfigurationEntryServerValue[] };
}

const setupMocks = (securityClearance: securityClearance) => {
    const { accessManager } = mockStore;
    const { manageServerService } = mockServices;

    accessManager.with_securityClearance(securityClearance);
    manageServerService.withServerSettings();
};

export const ServerSettingsStory: StoryObj<DefaultServerSettingsProps> = {
    name: "Server Settings",
    render: (args) => {
        setupMocks(args.securityClearance);

        return <ServerSettings />;
    },
    argTypes: {
        securityClearance: securityClearanceArgType,
    },
    args: {
        securityClearance: "ClusterAdmin",
        serverSettings: ManageServerStubs.serverSettings(),
    },
};
