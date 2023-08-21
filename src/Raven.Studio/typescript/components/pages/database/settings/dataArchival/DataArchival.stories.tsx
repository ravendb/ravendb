import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DataArchival from "./DataArchival";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: DataArchival,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DataArchival>;

export const DefaultDataArchival: StoryObj<typeof DataArchival> = {
    name: "Data Archival",
    render: () => {
        const { databasesService } = mockServices;
        databasesService.withDataArchivalConfiguration();
        return <DataArchival db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
