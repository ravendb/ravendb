import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ServerWideSorters from "./ServerWideSorters";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/ManageServer",
    component: ServerWideSorters,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ServerWideSorters>;

export const DefaultServerWideSorters: StoryObj<typeof ServerWideSorters> = {
    name: "Server-Wide Sorters",
    render: () => {
        return <ServerWideSorters db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
