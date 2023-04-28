import React from "react";
import CreateSampleData from "./CreateSampleData";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Tasks/CreateSampleData",
    decorators: [withStorybookContexts, withBootstrap5],
};

// prism highlight does not work in Storybook
export function FullView() {
    const { tasksService } = mockServices;

    tasksService.withGetSampleDataClasses();

    return <CreateSampleData db={DatabasesStubs.nonShardedClusterDatabase()} />;
}
