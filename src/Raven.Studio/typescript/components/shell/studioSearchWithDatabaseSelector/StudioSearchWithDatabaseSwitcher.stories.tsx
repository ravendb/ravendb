import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioSearchWithDatabaseSwitcher from "./StudioSearchWithDatabaseSwitcher";

export default {
    title: "Shell/StudioSearchWithDatabaseSwitcher",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const DefaultStory: StoryObj<{ isDatabaseSelected: boolean }> = {
    name: "Studio Search With Database Switcher",
    render: (args) => {
        const { databasesService, indexesService, tasksService } = mockServices;
        const { databases, collectionsTracker } = mockStore;

        databases.withDatabases([
            DatabasesStubs.shardedDatabase().toDto(),
            DatabasesStubs.nonShardedClusterDatabase().toDto(),
        ]);

        if (args.isDatabaseSelected) {
            databases.withActiveDatabase();
            databasesService.withDocumentsMetadataByIDPrefix();
            indexesService.withGetSampleStats();
            tasksService.withGetTasks();
            collectionsTracker.with_Collections();
        }

        return <StudioSearchWithDatabaseSwitcher />;
    },
    args: {
        isDatabaseSelected: true,
    },
};
