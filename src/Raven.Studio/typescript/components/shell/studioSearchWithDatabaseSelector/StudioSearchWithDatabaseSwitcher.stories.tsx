import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioSearchWithDatabaseSwitcher from "./StudioSearchWithDatabaseSwitcher";
import { DatabaseSharedInfo } from "components/models/databases";
import generateMenuItems from "common/shell/menu/generateMenuItems";

export default {
    title: "Shell/Studio Search With Database Switcher",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=20-73",
        },
    },
} satisfies Meta;

interface StoryArgs {
    hasMenuItems: boolean;
    isDatabaseSelected: boolean;
    isNewVersionAvailable: boolean;
    isWhatsNewVisible: boolean;
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
            databases.withActiveDatabase(db1);
            databasesService.withDocumentsMetadataByIDPrefix();
            indexesService.withGetSampleStats();
            tasksService.withGetTasks();
            collectionsTracker.with_Collections();
        }

        databases.withDatabases([db1, db2, db3]);

        const menuItems = args.hasMenuItems
            ? generateMenuItems({
                  db: args.isDatabaseSelected ? db1.name : "",
                  isNewVersionAvailable: args.isNewVersionAvailable,
                  isWhatsNewVisible: args.isWhatsNewVisible,
              })
            : [];

        return <StudioSearchWithDatabaseSwitcher menuItems={menuItems} />;
    },
    args: {
        hasMenuItems: true,
        isNewVersionAvailable: false,
        isWhatsNewVisible: false,
        isDatabaseSelected: true,
    },
};
