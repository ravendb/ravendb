import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import AdminJsConsole from "./AdminJsConsole";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Manage Server/Admin Js Console",
    component: AdminJsConsole,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof AdminJsConsole>;

export const DefaultAdminJSConsole: StoryObj<typeof AdminJsConsole> = {
    name: "Admin Js Console",
    render: () => {
        const clusterDb = DatabasesStubs.nonShardedClusterDatabase().toDto();
        const shardedDb = DatabasesStubs.shardedDatabase().toDto();

        mockStore.databases.withDatabases([clusterDb, shardedDb]);

        return <AdminJsConsole />;
    },
};
