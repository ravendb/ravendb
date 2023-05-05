import React from "react";
import CreateSampleData from "./CreateSampleData";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import MockTasksService from "test/mocks/services/MockTasksService";
import { ComponentMeta, ComponentStory } from "@storybook/react";

export default {
    title: "Pages/Database/Tasks/CreateSampleData",
    component: CreateSampleData,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof CreateSampleData>;

function commonInit(): MockTasksService {
    const { tasksService } = mockServices;

    tasksService.withGetSampleDataClasses();

    return tasksService;
}

export const DatabaseWithoutDocuments: ComponentStory<typeof CreateSampleData> = () => {
    const tasksService = commonInit();

    tasksService.withFetchCollectionsStats();

    return <CreateSampleData db={DatabasesStubs.nonShardedClusterDatabase()} />;
};

export const DatabaseWithDocuments: ComponentStory<typeof CreateSampleData> = () => {
    const tasksService = commonInit();

    tasksService.withFetchCollectionsStats(TasksStubs.notEmptyCollectionsStats());

    return <CreateSampleData db={DatabasesStubs.nonShardedClusterDatabase()} />;
};
