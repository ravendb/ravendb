import { Meta, StoryFn, StoryObj } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ServerSettings } from "./ServerSettings";

export default {
    title: "Pages/ManageServer/Server Settings",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface DefaultServerSettingsProps {}

export const ServerSettingsStory: StoryObj<DefaultServerSettingsProps> = {
    name: "Server Settings",
    render: (props: DefaultServerSettingsProps) => {
        return <ServerSettings />;
    },
};
