import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DataArchival from "./DataArchival";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Data Archival",
    component: DataArchival,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DataArchival>;

export const DefaultDataArchival: StoryObj<typeof DataArchival> = {
    name: "Data Archival",
    render: () => {
        const { databasesService } = mockServices;
        const { license } = mockStore;
        databasesService.withDataArchivalConfiguration();
        license.with_License();
        return <DataArchival db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};

export const LicenseRestricted: StoryObj<typeof DataArchival> = {
    render: () => {
        const { databasesService } = mockServices;
        const { license } = mockStore;
        databasesService.withDataArchivalConfiguration();
        license.with_License({ Type: "Community" });
        return <DataArchival db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
