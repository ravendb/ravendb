import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DatabaseCustomSorters from "./DatabaseCustomSorters";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings",
    component: DatabaseCustomSorters,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DatabaseCustomSorters>;

export const DefaultCustomSorters: StoryObj<typeof DatabaseCustomSorters> = {
    name: "Custom Sorters",
    render: () => {
        return <DatabaseCustomSorters db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
