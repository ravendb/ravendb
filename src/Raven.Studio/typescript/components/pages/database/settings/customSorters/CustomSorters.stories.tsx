import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import CustomSorters from "./CustomSorters";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: CustomSorters,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof CustomSorters>;

export const DefaultCustomSorters: StoryObj<typeof CustomSorters> = {
    name: "Custom Sorters",
    render: () => {
        return <CustomSorters db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
