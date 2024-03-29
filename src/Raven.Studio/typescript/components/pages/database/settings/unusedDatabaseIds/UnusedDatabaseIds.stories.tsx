import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, forceStoryRerender } from "test/storybookTestUtils";
import UnusedDatabaseIds from "./UnusedDatabaseIds";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface UnusedDatabaseIdsStoryArgs {
    isEmpty: boolean;
}

export const UnusedDatabaseIdsStory: StoryObj<UnusedDatabaseIdsStoryArgs> = {
    name: "Unused Database IDs",
    render: (args) => {
        const { databases } = mockStore;
        const { databasesService } = mockServices;

        databases.withActiveDatabase_NonSharded_Cluster();

        if (!args.isEmpty) {
            databasesService.withDatabaseRecord((x) => {
                (x as any).UnusedDatabaseIds = ["JLWI6JFUrvGgQAdujNyhBq", "LT7eFjLXwwPtsqRLSITbbp"];
            });
            databasesService.withDatabaseStats();
        }

        return <UnusedDatabaseIds {...forceStoryRerender()} />;
    },
    args: {
        isEmpty: false,
    },
};
