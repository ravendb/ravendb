import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioSearchWithDatabaseSwitcher from "./StudioSearchWithDatabaseSwitcher";
import { DatabaseSharedInfo } from "components/models/databases";

export default {
    title: "Shell/StudioSearchWithDatabaseSwitcher",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface StoryArgs {
    isDatabaseSelected: boolean;
}

export const DefaultStory: StoryObj<StoryArgs> = {
    name: "Studio Search With Database Switcher",
    render: (args) => {
        const { databasesService, indexesService, tasksService } = mockServices;
        const { databases, collectionsTracker } = mockStore;

        const db1: DatabaseSharedInfo = {
            ...DatabasesStubs.nonShardedSingleNodeDatabase().toDto(),
            name: "db1_nonSharded",
            environment: "Production",
        };
        const db2: DatabaseSharedInfo = {
            ...DatabasesStubs.nonShardedSingleNodeDatabase().toDto(),
            name: "db2_nonSharded",
            environment: "Testing",
            isDisabled: true,
        };
        const db3: DatabaseSharedInfo = {
            ...DatabasesStubs.shardedDatabase().toDto(),
            name: "db3_sharded",
            environment: "Development",
        };

        if (args.isDatabaseSelected) {
            databases.withActiveDatabase();
            databasesService.withDocumentsMetadataByIDPrefix();
            indexesService.withGetSampleStats();
            tasksService.withGetTasks();
            collectionsTracker.with_Collections();
        }

        databases.withDatabases([db1, db2, db3]);

        return <StudioSearchWithDatabaseSwitcher />;
    },
    args: {
        isDatabaseSelected: true,
    },
};
