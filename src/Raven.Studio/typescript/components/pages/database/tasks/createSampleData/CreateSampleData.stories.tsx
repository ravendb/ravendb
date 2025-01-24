import React from "react";
import CreateSampleData from "./CreateSampleData";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import MockTasksService from "test/mocks/services/MockTasksService";
import { Meta, StoryFn } from "@storybook/react";
import { mockStore } from "test/mocks/store/MockStore";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Tasks/Create Sample Data",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

function commonInit(): MockTasksService {
    const { tasksService } = mockServices;
    const { databases } = mockStore;

    databases.withActiveDatabase_NonSharded_SingleNode();
    tasksService.withGetSampleDataClasses();

    const nonShardedDatabase = DatabasesStubs.nonShardedSingleNodeDatabase();
    nonShardedDatabase.hasRevisionsConfiguration = ko.observable(true);
    activeDatabaseTracker.default.database(nonShardedDatabase);

    return tasksService;
}

export const DatabaseWithoutDocuments: StoryFn = () => {
    const tasksService = commonInit();

    tasksService.withFetchCollectionsStats();

    return <CreateSampleData />;
};

export const DatabaseWithDocuments: StoryFn = () => {
    const tasksService = commonInit();

    tasksService.withFetchCollectionsStats(TasksStubs.notEmptyCollectionsStats());

    return <CreateSampleData />;
};
