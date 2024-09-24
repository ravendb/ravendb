import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import TombstonesState from "./TombstonesState";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const Tombstones: StoryObj = {
    render: () => {
        const { databasesService } = mockServices;
        const { databases } = mockStore;

        databasesService.withTombstonesState();
        databases.withActiveDatabase_NonSharded_SingleNode();

        return <TombstonesState />;
    },
};
