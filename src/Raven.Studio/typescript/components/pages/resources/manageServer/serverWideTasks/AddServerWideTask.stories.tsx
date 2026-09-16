import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import AddServerWideTask from "./AddServerWideTask";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Manage Server/Server-Wide Tasks/Add Server-Wide Task",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/YCZpRbhT7UBIUJHDVzW50g/Pages---Server-Wide-Tasks?node-id=3-32912",
        },
    },
} satisfies Meta;

interface AddServerWideTaskStoryArgs {
    hasServerWideBackups: boolean;
    hasServerWideExternalReplications: boolean;
}

export const AddServerWideTaskStory: StoryObj<AddServerWideTaskStoryArgs> = {
    name: "Add Server-Wide Task",
    render: (props: AddServerWideTaskStoryArgs) => {
        const { license } = mockStore;
        license.with_LicenseLimited({
            HasServerWideBackups: props.hasServerWideBackups,
            HasServerWideExternalReplications: props.hasServerWideExternalReplications,
        });

        return <AddServerWideTask />;
    },
    args: {
        hasServerWideBackups: true,
        hasServerWideExternalReplications: true,
    },
};
