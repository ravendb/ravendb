import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioSearchWithDatabaseSwitcher from "./StudioSearchWithDatabaseSwitcher";

export default {
    title: "Shell/StudioSearchWithDatabaseSwitcher",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const DefaultStory = {
    name: "Studio Search With Database Switcher",
    render: () => {
        const { databasesService, indexesService, tasksService } = mockServices;
        const { databases, collectionsTracker } = mockStore;

        databases.withDatabases([
            DatabasesStubs.shardedDatabase().toDto(),
            DatabasesStubs.nonShardedClusterDatabase().toDto(),
        ]);

        // comment all to toggle active database
        databases.withActiveDatabase();
        databasesService.withDocumentsMetadataByIDPrefix();
        indexesService.withGetSampleStats();
        tasksService.withGetTasks();
        collectionsTracker.with_Collections();
        // end comment

        return <StudioSearchWithDatabaseSwitcher />;
    },
};
