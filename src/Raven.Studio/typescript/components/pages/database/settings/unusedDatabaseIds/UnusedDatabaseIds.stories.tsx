import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import UnusedDatabaseIds from "./UnusedDatabaseIds";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Settings/Unused Database IDs",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/j1zbg58w7AV02xYepTvW0W/Pages---Unused-Database-IDs?node-id=0-1&t=KhvkOq4Kt9aIss7U-1",
        },
    },
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

        return <UnusedDatabaseIds />;
    },
    args: {
        isEmpty: false,
    },
};
