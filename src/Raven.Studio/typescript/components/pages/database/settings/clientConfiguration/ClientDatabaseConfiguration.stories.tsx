import React from "react";
import { Meta, StoryFn } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ClientDatabaseConfiguration from "./ClientDatabaseConfiguration";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/ClientConfiguration",
    component: ClientDatabaseConfiguration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ClientDatabaseConfiguration>;

function commonInit() {
    const { accessManager, license, databases } = mockStore;
    const { manageServerService } = mockServices;

    databases.withActiveDatabase_NonSharded_SingleNode();
    accessManager.with_securityClearance("ClusterAdmin");
    license.with_License();
    manageServerService.withGetDatabaseClientConfiguration();
}

export const WithGlobalConfiguration: StoryFn<typeof ClientDatabaseConfiguration> = () => {
    commonInit();

    const { manageServerService } = mockServices;
    manageServerService.withGetGlobalClientConfiguration();

    return <ClientDatabaseConfiguration />;
};

export const WithoutGlobalConfiguration: StoryFn<typeof ClientDatabaseConfiguration> = () => {
    commonInit();

    const { manageServerService } = mockServices;
    manageServerService.withThrowingGetGlobalClientConfiguration();

    return <ClientDatabaseConfiguration />;
};

export const LicenseRestricted: StoryFn<typeof ClientDatabaseConfiguration> = () => {
    commonInit();

    const { manageServerService } = mockServices;
    const { license } = mockStore;

    manageServerService.withGetGlobalClientConfiguration();
    license.with_LicenseLimited({ HasClientConfiguration: false });

    return <ClientDatabaseConfiguration />;
};
