import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import TombstonesState from "./TombstonesState";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: TombstonesState,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof TombstonesState>;

export const Tombstones: StoryObj<typeof TombstonesState> = {
    render: () => {
        const { databasesService } = mockServices;
        databasesService.withTombstonesState();

        return <TombstonesState db={DatabasesStubs.nonShardedClusterDatabase()} location={{ nodeTag: "A" }} />;
    },
};
