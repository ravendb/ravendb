import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import ServerWideTasks from "./ServerWideTasks";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Manage Server/Server-Wide Tasks/Server-Wide Tasks",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/YCZpRbhT7UBIUJHDVzW50g/Pages---Server-Wide-Tasks?node-id=30-8017",
        },
    },
} satisfies Meta;

interface ServerWideTasksStoryArgs {
    isEmpty: boolean;
}

export const ServerWideTasksStory: StoryObj<ServerWideTasksStoryArgs> = {
    name: "Server-Wide Tasks",
    render: (props: ServerWideTasksStoryArgs) => {
        const { manageServerService } = mockServices;
        
        manageServerService.withServerWideTasks(props.isEmpty ? { Tasks: [] } : undefined);

        return <ServerWideTasks />;
    },
    args: {
        isEmpty: false,
    },
};
